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
import org.apache.hadoop.io._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.fs._;
import org.apache.hadoop.util._;
import org.apache.hadoop.mapred._;
import scala.reflect.Manifest;

import Magic._;
import Hadoop._;

abstract class AbstractPairs[K,V](protected val h: Hadoop)(implicit mK: Manifest[K], mV:Manifest[V]) extends DistributedPairs[K,V] with FileFormat[K,V] { self =>
  protected def paths : Array[Path];

  def elements = {
    if(paths.length == 0) 
      new Iterator[(K,V)] { 
        def hasNext = false;
        def next = throw new IllegalArgumentException("No elements were found!")
      }
    else paths.map(loadIterator).reduceLeft(_++_);
  }

  def force = new PathPairs(h,paths);

  /**
   * Models MapReduce/Hadoop-style reduce more exactly.
   */
  def flatReduce[K2,V2](f : (K,Iterator[V])=>Iterator[(K2,V2)])(implicit m : Manifest[K2], mU:Manifest[V2]): DistributedPairs[K2,V2] = {
    new MapReducePairs(h, self.paths, new PairTransformMapper(identity[Iterator[(K,V)]]), new FlatReduce(f), inputFormatClass);
  }

  /**
  * Models MapReduce/Hadoop-style reduce more exactly.
  */
  def reduce[K2,V2](f: (K,Iterator[V])=>(K2,V2))(implicit mL: Manifest[K2], mW:Manifest[V2]): DistributedPairs[K2,V2] = {
    new MapReducePairs(h, self.paths, new PairTransformMapper(identity[Iterator[(K,V)]]), new PairReduce(f), inputFormatClass);
  }

  /**
  * Lazy
  */
  override def map[K2,V2](f : ((K,V))=>(K2,V2))(implicit mJ : Manifest[K2], mU : Manifest[V2]): DistributedPairs[K2,V2] = {
    new ProjectedIterable[K2,V2](Util.itMap(f));
  }

 /**
  * Lazy
  */
 override def flatMap[K2,V2](f : ((K,V))=>Iterable[(K2,V2)])(implicit mJ : Manifest[K2], mU : Manifest[V2]): DistributedPairs[K2,V2] = {
   new ProjectedIterable[K2,V2](Util.itFlatMap(f));
 }

 /**
  * Lazy
  */
  override def filter(f : ((K,V))=>Boolean) : DistributedPairs[K,V] = new ProjectedIterable(Util.itFilter[(K,V)](f));

 /**
  * Lazy
  */
  override def mapFirst[K2](f : K=>K2)(implicit mJ: Manifest[K2]) : DistributedPairs[K2,V] = {
    new ProjectedIterable(Util.itMap { case (k,v) => (f(k),v)});
  }

  /**
  * Lazy
  */
  override def mapSecond[V2](f : V=>V2)(implicit mJ: Manifest[V2]) : DistributedPairs[K,V2] = {
    new ProjectedIterable(Util.itMap{ case (k,v) => (k,f(v))});
  }

  // Begin protected definitions
  /**
   * Loads the given path and returns and iterator that can read off objects. Defaults to SequenceFile's.
   */
  override protected def loadIterator(p : Path): Iterator[(K,V)] = {
    val rdr = new SequenceFile.Reader(p.getFileSystem(h.conf),p,h.conf);
    val keyType = rdr.getKeyClass().asSubclass(classOf[Writable]);
    val valType = rdr.getValueClass().asSubclass(classOf[Writable]);
    Util.iteratorFromProducer {() => 
      val k = keyType.newInstance();
      val v = valType.newInstance();
      if(rdr.next(k,v))  {
        Some((wireToReal(k).asInstanceOf[K],wireToReal(v).asInstanceOf[V]));
      } else {
        rdr.close(); 
        None;
      }  
    }
  }

  /**
   * Returns the InputFormat needed to read a file
   */
  override protected implicit def inputFormatClass : Class[T] forSome{ type T <: InputFormat[_,_]} = {
    classOf[SequenceFileInputFormat[_,_]].asInstanceOf[Class[InputFormat[_,_]]];
  }
    
  /**
  * Represents a transformation on the data.
  * Caches transform when "force" or "elements" is called.
  */
  private class ProjectedIterable[K2,V2](transform:Iterator[(K,V)]=>Iterator[(K2,V2)])(implicit mJ:Manifest[K2], mU: Manifest[V2]) extends DistributedPairs[K2,V2] {
    import Implicits._;
    def elements = force.elements;

    // TODO: better to slow down one machine than repeat unnecessary work on the cluster?
    // seems reasonable.
    def force(): DistributedPairs[K2,V2] = synchronized {
      cache match {
        case Some(output)=> (new PathPairs(h,output))
        case None =>
        val output = h.runMapReduce(paths,
                                    new PairTransformMapper(transform),
                                    new IdentityReduce[K2,V2]());
        cache = Some(output);
        (new PathPairs(h,output))
      }
    }

    def asStage(output : String):DistributedPairs[K2,V2] = {
      implicit val conf = h.conf;
      val outDir = h.dirGenerator(output);
      if(outDir.exists) {
        cache = Some(outDir.listFiles);
        this;
      } else synchronized {
        val outFiles = h.runMapReduce(paths,
                                    new PairTransformMapper(transform),
                                    new IdentityReduce[K2,V2](),
                                    Set(OutputDir(output)));
        cache = Some(outFiles);
        (new PathPairs[K2,V2](h,outFiles))
      }
    }

    /// So we don't repeat a computation unncessarily
    private var _cache : Option[Array[Path]] = None;

    // must be synchronized
    private def cache = synchronized { _cache };
    private def cache_=(c : Option[Array[Path]]) = c;

    override def map[K3,V3](f : ((K2,V2))=>(K3,V3))(implicit mL: Manifest[K3], mW: Manifest[V3]): DistributedPairs[K3,V3] = cache match {
      case Some(path) => new PathPairs[K2,V2](h,path).map(f);
      case None => new ProjectedIterable[K3,V3](Util.andThen(transform, Util.itMap(f)));
    }

    override def flatMap[K3,V3](f : ((K2,V2))=>Iterable[(K3,V3)])(implicit mL: Manifest[K3], mW: Manifest[V3]) : DistributedPairs[K3,V3] = cache match {
      case Some(path) => new PathPairs[K2,V2](h,path).flatMap(f);
      case _ => new ProjectedIterable[K3,V3](Util.andThen(transform,Util.itFlatMap(f)));
    }

    override def filter(f : ((K2,V2))=>Boolean) : DistributedPairs[K2,V2] = cache match {
      case Some(path) => new PathPairs[K2,V2](h,path).filter(f);
      case None => new ProjectedIterable[K2,V2](Util.andThen(transform,Util.itFilter(f)));
    }

    /**
    * Lazy
    */
    override def mapFirst[K3](f : K2=>K3)(implicit mL: Manifest[K3]) : DistributedPairs[K3,V2] = {
      new ProjectedIterable(Util.andThen(transform,Util.itMap[(K2,V2),(K3,V2)]{ case (k,v) => (f(k),v)}));
    }

    /**
    * Lazy
    */
    override def mapSecond[V3](f : V2=>V3)(implicit mW: Manifest[V3]) : DistributedPairs[K2,V3] = {
      new ProjectedIterable(Util.andThen(transform,Util.itMap[(K2,V2),(K2,V3)]{ case (k,v) => (k,f(v))}));
    }

    /**
    * Models MapReduce/Hadoop-style reduce more exactly.
    */
    def flatReduce[K3,V3](f : (K2,Iterator[V2])=>Iterator[(K3,V3)])(implicit mK3 : Manifest[K3], mV3:Manifest[V3]): DistributedPairs[K3,V3] = {
      new MapReducePairs(h, self.paths, new PairTransformMapper(transform), new FlatReduce(f), inputFormatClass);
    }

    /**
    * Models MapReduce/Hadoop-style reduce more exactly.
    */
    def reduce[K3,V3](f: (K2,Iterator[V2])=>(K3,V3))(implicit mL: Manifest[K3], mW:Manifest[V3]): DistributedPairs[K3,V3] = {
      new MapReducePairs(h, self.paths, new PairTransformMapper(transform), new PairReduce(f), inputFormatClass);
    }
  }
    
}
private class MapReducePairs[K1,V1,K2,V2,K3,V3](h : Hadoop,
  input: =>Array[Path],
  m : Mapper[K1,V1,K2,V2],
  r : Reduce[K2,V2,K3,V3],
  val inputFormat : Class[T] forSome {type T <: InputFormat[_,_]})
  (implicit mk1 : Manifest[K1], mk2 : Manifest[K2], mk3:Manifest[K3],
    mv1:Manifest[V1], mv2:Manifest[V2], mv3 : Manifest[V3]) extends AbstractPairs[K3,V3](h) {

    import Implicits._;
    private implicit val conf = h.conf;

    override lazy val paths = {
      h.runMapReduce(input, m,r);
    }

    override def asStage(dir : String) : DistributedPairs[K3,V3] = {
      val outDir = h.dirGenerator(dir);
      if(outDir.exists) {
        new PathPairs(h,outDir.listFiles);
      } else synchronized {
        val outFiles = h.runMapReduce(input, m,r, Set(OutputDir(dir)));
        (new PathPairs[K3,V3](h,outFiles))
      }
    }

    override implicit def inputFormatClass : Class[_ <: InputFormat[_,_]] = inputFormat;

  }

class PathPairs[K,V](h: Hadoop, val paths : Array[Path])(implicit mK: Manifest[K], mV:Manifest[V]) extends AbstractPairs[K,V](h) {
  import Implicits._;
  /**
  * Copies the files represented by the pathpairs to the stage directory.
  */
  def asStage(output: String) = {
    implicit val conf = h.conf;
    val outputDir = h.dirGenerator(output);
    outputDir.mkdirs();
    val outPaths = for(p <- paths) yield new Path(outputDir,p.getName);
    new PathPairs(h,outPaths);
  }
}


/**
* Used to override the default behavior of Lines
*/
trait FileFormat[K,V] { 
  protected def loadIterator(p: Path): Iterator[(K,V)]
  protected def inputFormatClass : Class[T] forSome { type T <: InputFormat[_,_]}
}

/**
* Used with PathPairs, reads files line by line. Key is the offset in bytes
*/
trait Lines extends FileFormat[Long,String]{ this : PathPairs[Long,String] =>
  import Implicits._;
  override protected def loadIterator(p: Path) = {
    implicit val conf = h.conf;

    val rdr = new LineRecordReader(p.getFileSystem(h.conf).open(p),0,p.length);
    val k = new LongWritable;
    val v = new Text;
    Util.iteratorFromProducer { () =>
      if(rdr.next(k,v)) {
        Some((k.get,v.toString));
      } else {
        rdr.close;
        None;
      }
    }
  }

  override protected def inputFormatClass = {
    classOf[TextInputFormat].asInstanceOf[Class[InputFormat[_,_]]];
  }
}


