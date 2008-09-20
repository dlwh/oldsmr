/*
* Copyright (c) 2008, David Hall
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*
* THIS SOFTWARE IS PROVIDED BY DAVID HALL ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL DAVID HALL BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package smr.hadoop;
import smr._;

import java.io._;

import org.apache.hadoop.io._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.fs._;
import org.apache.hadoop.util._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.filecache._;

import scala.reflect.Manifest;


/**
 * Supports Hadoop operations.
 * @see Hadoop$
 */
class Hadoop(val conf : Configuration, private[hadoop] val dirGenerator : (String)=>Path) {
  // enable path conversions, and other goodies
  implicit private val cf = conf;
  import Implicits._;
  import Hadoop._;

  /**
   * Constructs a Hadoop instance with the given configuration and working directory (for files)
   */
  def this(conf : Configuration, workDir : Path) = this(conf,{(pref:String) =>
    new Path(workDir,pref);
  });

  private[smr] val cacheDir = dirGenerator("tmp/cache");

  conf.set("smr.cache.dir",cacheDir.toString);
  cacheDir.mkdirs();
  if(!conf.getBoolean(CONFIG_KEEP_FILES,false))
    dirGenerator("tmp").deleteOnExit();

  def load[T](p : Array[Path])(implicit m : Manifest[T])= new PathIterable[T](this,p);
  def load[T](p : Path)(implicit m : Manifest[T]):PathIterable[T]= load[T](Array(p));

  def loadPairs[K,V](p : Path*)(implicit mK : Manifest[K], mV: Manifest[V]) = {
    new PathPairs[K,V](this,p.toArray);
  }

  def loadPairs[K,V](p : Seq[Path]) = new PathPairs(this,p.toArray);
  def loadLines(p : Path*) = new PathPairs[Long,String](this,p.toArray) with Lines;

  import Magic._;

  def distributePairs[K,V](ibl: Iterable[(K,V)], numShards : Int)(implicit mK:Manifest[K], mV:Manifest[V]) = {
    val paths = pathGenerator(numShards);
    val elems = ibl.elements.map{ case(k,v) => (realToWire(k),realToWire(v))}

    if(!elems.hasNext) 
      throw new IllegalArgumentException("Empty iterable");
    val first = elems.next;  

    val writers = 
      for(p <- paths;
        fs = p.getFileSystem(conf);
        wrtr = new SequenceFile.Writer(fs,conf,p,first._1.getClass,first._2.getClass)) 
      yield wrtr;
    var i = 0;
    writers(i%numShards).append(first._1,first._2);
    while(elems.hasNext) {
      i+=1;
      val nxt = elems.next();
      writers(i%numShards).append(nxt._1,nxt._2);
    }
    writers.foreach{_.close()};
    loadPairs[K,V](paths).asInstanceOf[PathPairs[K,V]];
  }

  def distribute[T](ibl : Iterable[T], numShards :Int)(implicit m : Manifest[T]) :PathIterable[T] = {
    val paths = pathGenerator(numShards);

    val elems = ibl.elements.map(Magic.realToWire);

    if(!elems.hasNext) 
      throw new IllegalArgumentException("Empty iterable");
    val first = elems.next;  

    val writers = 
      for(p <- paths;
        fs = p.getFileSystem(conf);
        wrtr = new SequenceFile.Writer(fs,conf,p,classOf[Hadoop.DefaultKeyWritable],first.getClass)) 
      yield wrtr;
    var i = 0;
    writers(i%numShards).append(Magic.realToWire(mkDefaultKey(first)),first);
    while(elems.hasNext) {
      i+=1;
      val nxt = elems.next();
      writers(i%numShards).append(Magic.realToWire(mkDefaultKey(nxt)),nxt);
    }
    writers.foreach{_.close()};
    load[T](paths);
  }

  private def serializeClass(jobConf : JobConf, name : String, c : AnyRef) = {
    implicit val jc = jobConf;
    val path = new Path(cacheDir,name);
    val stream = new ObjectOutputStream(path.getFileSystem(jc).create(path));
    stream.writeObject(c);
    stream.close();
    DistributedCache.addCacheFile(path.toUri,jobConf);
    path;
  }

  private[hadoop] def runMapReduce[K1,V1,K2,V2,K3,V3](paths : Array[Path],
    m: Mapper[K1,V1,K2,V2],
    r: Reduce[K2,V2,K3,V3]) 
    (implicit mk2:Manifest[K2], mv2:Manifest[V2],
             mk3:Manifest[K3], mv3:Manifest[V3],
             inputFormat : Class[T] forSome {type T <: InputFormat[_,_]}) : Array[Path]= {
    runMapReduce(paths,m,r,Set());
  }

  private[hadoop] def runMapReduce[K1,V1,K2,V2,K3,V3](paths : Array[Path],
    m: Mapper[K1,V1,K2,V2],
    r: Reduce[K2,V2,K3,V3],
    options : Set[Hadoop.Options]) 
   (implicit mk2:Manifest[K2], mv2:Manifest[V2],
             mk3:Manifest[K3], mv3:Manifest[V3],
             inputFormat : Class[T] forSome {type T <: InputFormat[_, _]}) = {
    implicit val jobConf = new JobConf(conf, m.getFunClass); 

    var outputOption : Option[Path] =  None;
    options foreach {
      case ReduceCombine => jobConf.setCombinerClass(classOf[ReduceWrapper[_,_,_,_]]);
      case OutputDir(dir) => outputOption = Some(dirGenerator(dir));
      case x => throw new IllegalArgumentException("Illegal MapReduce Option: " + x);
    }

    val outputPath = outputOption.getOrElse(genDir);
    jobConf.setJobName("SMR-"+outputPath.getName);
    jobConf.setInputFormat(inputFormat);
    jobConf.setOutputFormat(classOf[SequenceFileOutputFormat[_,_]]);

    val mPath = serializeClass(jobConf,outputPath.getName+"-Map.ser",m);
    val rPath = serializeClass(jobConf,outputPath.getName+"-Reduce.ser",r);
    jobConf.set("smr.job.mapper.file",mPath.toString);
    jobConf.set("smr.job.reducer.file",rPath.toString);

    jobConf.setMapRunnerClass(classOf[ClosureMapper[_,_,_,_]]);
    jobConf.setReducerClass(classOf[ReduceWrapper[_,_,_,_]]);
    
    jobConf.setMapOutputKeyClass(Magic.classToWritableClass(mk2.erasure));
    jobConf.setMapOutputValueClass(Magic.classToWritableClass(mv2.erasure));
    jobConf.setOutputKeyClass(Magic.classToWritableClass(mk3.erasure));
    jobConf.setOutputValueClass(Magic.classToWritableClass(mv3.erasure));
    
    FileInputFormat.setInputPaths(jobConf, paths:_*);
    FileOutputFormat.setOutputPath(jobConf,outputPath);

    JobClient.runJob(jobConf);

    outputPath.listFiles();
  }

  private var jobNum = 0;
  protected def nextName = synchronized { 
    jobNum+=1;
    "job"+jobNum;
  }

  private def genDir() = {
    dirGenerator("tmp/"+nextName);
  }

  private def pathGenerator(numShards : Int) = {
    val dir = genDir();
    dir.mkdirs();

    Array.fromFunction { i => 
      new Path(dir,"part-"+i+"-of-"+numShards);
    } (numShards);
  }
}

object Hadoop {
  /**
   * Create a {@link Hadoop} instance from command line args and a working directory.
   */
  def apply(args : Array[String], workDir : Path) = fromArgs(args, workDir)._1;
  
  /**
   * Create a {@link Hadoop} instance from command line args and a working directory. 
   * @return hadoop instance and remaining args
   */
  def fromArgs(args: Array[String], workDir : Path)  = {
    var restArgs : Array[String] = null;
    var conf : Configuration = null;
    val tool = new Configured with Tool {
      @throws(classOf[Exception])
      def run(args : Array[String]) : Int = {
        restArgs = args;
        conf = getConf();
        0;
      }
    }
    ToolRunner.run(tool,args);
    (new Hadoop(conf,workDir),args);
  }

  private[hadoop] sealed case class Options;
  case object ReduceCombine extends Options;
  case class OutputDir(s : String) extends Options;

  private[hadoop] type DefaultKeyWritable = IntWritable;
  private[hadoop] type DefaultKey= Int;
  private[hadoop] def mkDefaultKey()  : DefaultKey= 0.asInstanceOf[DefaultKey];
  private[hadoop] def mkDefaultKey[V](v: V): DefaultKey = v.asInstanceOf[AnyRef].hashCode();

  val CONFIG_KEEP_FILES = "smr.files.keep";

  private def copyFile(inFile : Path, outFile : Path)(implicit conf : Configuration) {
    val fs = inFile.getFileSystem(conf);
    // Read from and write to new file
    val in = fs.open(inFile);
    val out = fs.create(outFile);
    val COPY_BUFFER_SIZE = 4096;
    val buffer = new Array[Byte](4096);
    try {
      var bytesRead = in.read(buffer);
      while (bytesRead > 0) {
        out.write(buffer, 0, bytesRead);
        bytesRead = in.read(buffer);
      }
    } finally {
      in.close();
      out.close();
    }
  }
}


