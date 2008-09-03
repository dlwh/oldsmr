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
import scala.reflect.Manifest;

/**
 * Represents SequenceFiles of (K,V) pairs on disk. 
 * All operations are scheduled as MapReduces using Hadoop.runMapReduce.
 *
 * Note that the underlying representation is distinct from {@link PathIerable}
 */
class PathPairs[K,V](h: Hadoop, val paths: Array[Path])(implicit mK: Manifest[K], mV:Manifest[V]) extends DistributedPairs[K,V] {
  import Magic._;

  def elements = {
    if(paths.length == 0) 
      new Iterator[(K,V)] { 
        def hasNext = false;
        def next = throw new IllegalArgumentException("No elements were found!")
      }
    else (for(p <- paths;
            rdr = new SequenceFile.Reader(p.getFileSystem(h.conf),p,h.conf);
            keyType = rdr.getKeyClass().asSubclass(classOf[Writable]);
            valType = rdr.getValueClass().asSubclass(classOf[Writable]))
          yield Util.iteratorFromProducer {() => 
            val k = keyType.newInstance();
            val v = valType.newInstance();
            if(rdr.next(k,v))  {
              Some((wireToReal(k).asInstanceOf[K],wireToReal(v).asInstanceOf[V]));
            } else {
              rdr.close(); 
              None;
            }
          }).reduceLeft(_++_);
  }

  def force = this;

  import Hadoop._;
  def reduce[A >: K, B>:V](f : ((A,B),(A,B))=>(A,B)): (A,B) = {
    implicit val a = mK.asInstanceOf[Manifest[A]];
    implicit val b = mV.asInstanceOf[Manifest[B]];

    val output = h.runMapReduce(paths, new PairCollectorMapper(identity[Iterator[(A,B)]]), new RealReduce(f), Set(ReduceCombine));
    val path = output(0);

    val result = new SequenceFile.Reader(path.getFileSystem(h.conf),path,h.conf);
    val v = result.getValueClass.asSubclass(classOf[Writable]).newInstance();
    val k = result.getKeyClass.asSubclass(classOf[Writable]).newInstance();
    result.next(k,v);
    result.close();
    wireToReal(v).asInstanceOf[(K,V)];
  }

  /**
   * Models MapReduce/Hadoop-style reduce more exactly.
   */
  def hreduce[J,U](f : (K,Iterator[V])=>Iterator[(J,U)])(implicit m : Manifest[J], mU:Manifest[U]): DistributedPairs[J,U] = {
    val output = h.runMapReduce(paths, new PairTransformMapper(identity[Iterator[(K,V)]]), new HReduce(f));
    new PathPairs(h,output);
  }

 /**
  * Lazy
  */
 override def map[J,U](f : ((K,V))=>(J,U))(implicit mJ : Manifest[J], mU : Manifest[U]): DistributedPairs[J,U] = {
   new ProjectedIterable[J,U](Util.itMap(f));
 }

 /**
  * Lazy
  */
 override def flatMap[J,U](f : ((K,V))=>Iterable[(J,U)])(implicit mJ : Manifest[J], mU : Manifest[U]): DistributedPairs[J,U] = {
   new ProjectedIterable[J,U](Util.itFlatMap(f));
 }

 /**
  * Lazy
  */
 override def filter(f : ((K,V))=>Boolean) : DistributedPairs[K,V] = new ProjectedIterable(Util.itFilter[(K,V)](f));
 /**
  * Lazy
  */
  override def mapFirst[J](f : K=>J)(implicit mJ: Manifest[J]) : DistributedPairs[J,V] = {
    new ProjectedIterable(Util.itMap { case (k,v) => (f(k),v)});
  }
 /**
  * Lazy
  */
  override def mapSecond[U](f : V=>U)(implicit mJ: Manifest[U]) : DistributedPairs[K,U] = {
    new ProjectedIterable(Util.itMap{ case (k,v) => (k,f(v))});
  }

 /**
  * Represents a transformation on the data.
  * Caches transform when "force" or "elements" is called.
  */
 private class ProjectedIterable[J,U](transform:Iterator[(K,V)]=>Iterator[(J,U)])(implicit mJ:Manifest[J], mU: Manifest[U]) extends DistributedPairs[J,U] {
    def elements = force.elements;

    // TODO: better to slow down one machine than repeat unnecessary work on the cluster?
    // seems reasonable.
    def force(): DistributedPairs[J,U] = synchronized {
      cache match {
        case Some(output)=> (new PathPairs(h,output))
        case None =>
        val output = h.runMapReduce(paths,
                                    new PairTransformMapper(transform),
                                    new IdentityReduce[J,U]());
        cache = Some(output);
        (new PathPairs(h,output))
      }
    }

    /// So we don't repeat a computation unncessarily
    private var _cache : Option[Array[Path]] = None;

    // must be synchronized
    private def cache = synchronized { _cache };
    private def cache_=(c : Option[Array[Path]]) = c;

    override def map[L,W](f : ((J,U))=>(L,W))(implicit mL: Manifest[L], mW: Manifest[W]): DistributedPairs[L,W] = cache match {
      case Some(path) => new PathPairs[J,U](h,path).map(f);
      case None => new ProjectedIterable[L,W](Util.andThen(transform, Util.itMap(f)));
    }

    override def flatMap[L,W](f : ((J,U))=>Iterable[(L,W)])(implicit mL: Manifest[L], mW: Manifest[W]) : DistributedPairs[L,W] = cache match {
      case Some(path) => new PathPairs[J,U](h,path).flatMap(f);
      case _ => new ProjectedIterable[L,W](Util.andThen(transform,Util.itFlatMap(f)));
    }

    override def filter(f : ((J,U))=>Boolean) : DistributedPairs[J,U] = cache match {
      case Some(path) => new PathPairs[J,U](h,path).filter(f);
      case None => new ProjectedIterable[J,U](Util.andThen(transform,Util.itFilter(f)));
    }

    /**
    * Lazy
    */
    override def mapFirst[L](f : J=>L)(implicit mL: Manifest[L]) : DistributedPairs[L,U] = {
      new ProjectedIterable(Util.andThen(transform,Util.itMap[(J,U),(L,U)]{ case (k,v) => (f(k),v)}));
    }

    /**
    * Lazy
    */
    override def mapSecond[W](f : U=>W)(implicit mW: Manifest[W]) : DistributedPairs[J,W] = {
      new ProjectedIterable(Util.andThen(transform,Util.itMap[(J,U),(J,W)]{ case (k,v) => (k,f(v))}));
    }

    def reduce[A>:J, B>:U](f: ((A,B),(A,B))=>(A,B)) : (A,B) = cache match { 
      case Some(path) => new PathPairs[J,U](h,path).reduce(f);
      case None =>
      implicit val a = mJ.asInstanceOf[Manifest[A]];
      implicit val b = mU.asInstanceOf[Manifest[B]];
      // XXX todo
      val output = h.runMapReduce(paths,
                                  new PairCollectorMapper(transform),
                                  new RealReduce(f),
                                  Set(ReduceCombine));
      val path = output(0);

      val result = new SequenceFile.Reader(path.getFileSystem(h.conf),path,h.conf);
      val v = result.getValueClass.asSubclass(classOf[Writable]).newInstance();
      val k = result.getKeyClass.asSubclass(classOf[Writable]).newInstance();
      result.next(k,v);
      result.close();
      wireToReal(v).asInstanceOf[(A,B)]
    }

    /**
    * Models MapReduce/Hadoop-style reduce more exactly.
    */
    def hreduce[L,W](f : (J,Iterator[U])=>Iterator[(L,W)])(implicit mL : Manifest[L], mW:Manifest[W]): DistributedPairs[L,W] = {
      val output = h.runMapReduce(paths, new PairTransformMapper(transform), new HReduce(f));
      new PathPairs(h,output);
    }

  }
}
