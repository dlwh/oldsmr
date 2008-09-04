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

/**
 * Represents SequenceFiles of (Hadoop.DefaultKey,T) pairs on disk. 
 * All operations are scheduled as MapReduces using Hadoop.runMapReduce.
 * The DefaultKey is inaccessible.
 */
class PathIterable[T](h: Hadoop, val paths: Array[Path])(implicit m: Manifest[T]) extends DistributedIterable[T] {
  import Magic._;
  def elements = {
    if(paths.length == 0) 
      new Iterator[T] { 
        def hasNext = false;
        def next = throw new IllegalArgumentException("No elements were found!")
      }
    else paths.map(loadIterator).reduceLeft(_++_)
  }

  def force = this;

  import Hadoop._;
  def reduce[B>:T](f: (B,B)=>B) : B = { 
    implicit val b = m.asInstanceOf[Manifest[B]];
    implicit val klass = inputFormatClass.asInstanceOf[Class[InputFormat[Any,B]]];
    val output = h.runMapReduce(paths, new CollectorMapper(identity[Iterator[B]]), new RealReduce(f), Set(ReduceCombine));
    val path = output(0);

    val result = new SequenceFile.Reader(path.getFileSystem(h.conf),path,h.conf);
    val v = result.getValueClass.asSubclass(classOf[Writable]).newInstance();
    val k = result.getKeyClass.asSubclass(classOf[Writable]).newInstance();
    result.next(k,v);
    result.close();
    Magic.wireToReal(v).asInstanceOf[B];
  }

  /**
   * Equivalent to Set() ++ it.elements, but distributed.
   */
  def distinct() = { 
    implicit val klass = inputFormatClass.asInstanceOf[Class[InputFormat[DefaultKey,T]]];
    val output = h.runMapReduce(paths,new SwapMapper[DefaultKey,T],new KeyToValReduce[T,DefaultKey]);
    new PathIterable(h,output);
  }

 /**
  * Lazy
  */
 override def map[U](f : T=>U)(implicit m : Manifest[U]): DistributedIterable[U] = new ProjectedIterable[U](Util.itMap(f));
 /**
  * Lazy
  */
 override def flatMap[U](f : T=>Iterable[U]) (implicit m : Manifest[U]): DistributedIterable[U] = new ProjectedIterable[U](Util.itFlatMap(f));
 /**
  * Lazy
  */
 override def filter(f : T=>Boolean) : DistributedIterable[T] = new ProjectedIterable(Util.itFilter[T](f));

  // Begin protected definitions
  /**
   * Loads the given path and returns and iterator that can read off objects. Defaults to SequenceFile's.
   */
  protected def loadIterator(p : Path): Iterator[T] = {
    val rdr = new SequenceFile.Reader(p.getFileSystem(h.conf),p,h.conf);
    val keyType = rdr.getKeyClass().asSubclass(classOf[Writable]);
    val valType = rdr.getValueClass().asSubclass(classOf[Writable]);
    Util.iteratorFromProducer {() => 
      val k = keyType.newInstance();
      val v = valType.newInstance();
      if(rdr.next(k,v))  {
        Some(wireToReal(v).asInstanceOf[T]);
      } else {
        rdr.close(); 
        None;
      }  
    }
  }

  /**
   * Returns the InputFormat needed to read a file
   */
  protected implicit def inputFormatClass : Class[C] forSome{ type C <: InputFormat[_,_]} = {
    classOf[SequenceFileInputFormat[_,_]]
  }

 /**
  * Represents a transformation on the data.
  * Caches transform when "force" or "elements" is called.
  */
 private class ProjectedIterable[U](transform:Iterator[T]=>Iterator[U])(implicit mU: Manifest[U]) extends DistributedIterable[U] {
    def elements = force.elements;

    // TODO: better to slow down one machine than repeat unnecessary work on the cluster?
    // seems reasonable.
    def force(): DistributedIterable[U] = synchronized {
      cache match {
        case Some(output)=> (new PathIterable(h,output)(mU))
        case None =>
        val output = h.runMapReduce(paths,
                                    new TransformMapper(transform),
                                    new IdentityReduce[DefaultKey,U]());
        cache = Some(output);
        (new PathIterable(h,output)(mU))
      }
    }

    /// So we don't repeat a computation unncessarily
    private var _cache : Option[Array[Path]] = None;

    // must be synchronized
    private def cache = synchronized { _cache };
    private def cache_=(c : Option[Array[Path]]) = c;

    override def map[V](f : U=>V)(implicit m: Manifest[V]): DistributedIterable[V] = cache match {
      case Some(path) => new PathIterable[U](h,path).map(f);
      case None => new ProjectedIterable[V](Util.andThen(transform, Util.itMap(f)));
    }

    override def flatMap[V](f : U=>Iterable[V])(implicit m: Manifest[V]) : DistributedIterable[V] = cache match {
      case Some(path) => new PathIterable[U](h,path).flatMap(f);
      case _ => new ProjectedIterable[V](Util.andThen(transform,Util.itFlatMap(f)));
    }

    override def filter(f : U=>Boolean) : DistributedIterable[U] = cache match {
      case Some(path) => new PathIterable[U](h,path).filter(f);
      case None => new ProjectedIterable[U](Util.andThen(transform,Util.itFilter(f)));
    }

    def distinct() = cache match { 
      case Some(path) => new PathIterable[U](h,path).distinct();
      case None =>
      val output = h.runMapReduce(paths,
                                  new TransformValMapper[DefaultKey,T,U](transform),
                                  new KeyToValReduce[U,DefaultKey]);
      new PathIterable(h,output);
    }

    def reduce[B>:U](f: (B,B)=>B) : B = cache match { 
      case Some(path) => new PathIterable[U](h,path).reduce(f);
      case None =>
      implicit val b = m.asInstanceOf[Manifest[B]];
      val output = h.runMapReduce(paths,
                                  new CollectorMapper(transform),
                                  new RealReduce(f),
                                  Set(ReduceCombine));
      val path = output(0);

      val result = new SequenceFile.Reader(path.getFileSystem(h.conf),path,h.conf);
      val v = result.getValueClass.asSubclass(classOf[Writable]).newInstance();
      val k = result.getKeyClass.asSubclass(classOf[Writable]).newInstance();
      result.next(k,v);
      result.close();
      wireToReal(v).asInstanceOf[B];
    }
  }

}


