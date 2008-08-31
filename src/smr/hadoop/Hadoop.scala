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


/**
 * Supports Hadoop operations.
 * @see Hadoop$
 */
class Hadoop(val conf : Configuration, dirGenerator : (String)=>Path) {
  // enable path conversions, and other goodies
  implicit private val cf = conf;
  import Implicits._;

  /**
   * Constructs a Hadoop instance with the given configuration and working directory (for files)
   */
  def this(conf : Configuration, workDir : Path) = this(conf,{(pref:String) =>
    new Path(workDir,pref);
  });

  private[smr] val cacheDir = dirGenerator("cache");

  println(cacheDir);
  conf.set("smr.cache.dir",cacheDir.toString);
  cacheDir.mkdirs();

  def load[T](p : Array[Path])= new PathIterable(this,p);
  def load[T](p : Path):PathIterable[T]= load[T](Array(p));

  def distribute[T](ibl : Iterable[T], numShards :Int)  = {
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
    val fakeKey = new Hadoop.DefaultKeyWritable();
    var i = 0;
    writers(i%numShards).append(fakeKey,first);
    while(elems.hasNext) {
      i+=1;
      val nxt = elems.next();
      writers(i%numShards).append(fakeKey,nxt);
    }
    writers.foreach{_.close()};
    load(paths);
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
    r: Reduce[K2,V2,K3,V3]) = {
    implicit val jobConf = new JobConf(conf, m.getFunClass); 

    val outputPath = genDir;

    jobConf.setJobName("SMR-"+outputPath.getName);

    val mPath = serializeClass(jobConf,outputPath.getName+"-Map.ser",m);
    val rPath = serializeClass(jobConf,outputPath.getName+"-Reduce.ser",r);
    jobConf.set("smr.job.mapper.file",mPath.toString);
    jobConf.set("smr.job.reducer.file",rPath.toString);

    jobConf.setMapRunnerClass(classOf[ClosureMapper[_,_,_,_]]);
    jobConf.setReducerClass(classOf[ReduceWrapper[_,_,_,_]]);

    jobConf.setInputFormat(classOf[SequenceFileInputFormat[_,_]])
    jobConf.setOutputFormat(classOf[SequenceFileOutputFormat[_,_]])

    FileInputFormat.setInputPaths(jobConf, paths);
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
    dirGenerator(nextName);
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



  private[hadoop] type DefaultKeyWritable = BooleanWritable;
  private[hadoop] type DefaultKey= Boolean;
}


// You know it's bad when you have a class called magic
object Magic {
  def wireToReal(t : Writable) :Any = t match {
    case t :Text => t.toString.asInstanceOf;
    case arr : ArrayWritable => arr.get().map(wireToReal);
    case t => try {
      t.asInstanceOf[{def get():Any;}].get();
    } catch {
      case e => t;
    }
  }

  implicit def realToWire(t : Any):Writable = t match {
    case t : Writable => t;
    case t : Int => new IntWritable(t);
    case t : Long => new LongWritable(t);
    case t : Float => new FloatWritable(t);
    case t : Double => new DoubleWritable(t);
    case t : Boolean => new BooleanWritable(t);
    case t : String => new Text(t);
    case t : Array[Byte] => new BytesWritable(t);
    case x : AnyRef if x.getClass.isArray => { 
      val t = x.asInstanceOf[Array[Any]];
      if(t.length == 0) new ObjectWritable(t);
      else { 
        val mapped = t.map(realToWire); 
        val classes = mapped.map(_.getClass);
        if(classes.forall(classes(0)==_)) { 
          // can only use ArrayWritable if all Writables are the same.
          new ArrayWritable(classes(0),mapped);
        } else {
          // fall back on ObjectWritable
          val mapped = t.map(new ObjectWritable(_).asInstanceOf[Writable]); 
          new ArrayWritable(classOf[ObjectWritable],mapped);
        }
      }
    }
    case _ => new ObjectWritable(t);
  }

}
