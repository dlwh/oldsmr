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
package smr;
import scala.collection.mutable.ArrayBuffer;

/**
 * Object to hold various sensible Defaults for SMR. Expected use:
 * <pre>
 * import smr.Defaults._;
 * </pre>
 * 
 * @author dlwh
 */
object Defaults extends SerializedImplicits {

  /**
   * Implicit shard function that provides a reasonable default in most cases. Special treatment for
   * Ranges and for Seqs
   */
  implicit def shard[T] (it : Iterable[T], numShards : Int) : List[Iterable[T]] = it match {
    case x : scala.Range.Inclusive => shardIRange(x,numShards).asInstanceOf[List[Iterable[T]]];
    case x : scala.Range=> shardRange(x,numShards).asInstanceOf[List[Iterable[T]]];
    case x : Seq[_] => 
    if(x.size < numShards) {
     List(x) 
    } else {
      val sz = x.size / numShards;
      val arrs = new ArrayBuffer[Iterable[T]]
      arrs ++= (for(val i <- 0 until numShards ) yield x.drop(sz * i).take(sz).toList);
      arrs.toList
    }
    case _ =>
    val arrs = new ArrayBuffer[ArrayBuffer[T]]
    arrs ++= (for(val i <- 1 to numShards) yield new ArrayBuffer[T]);
    val elems = it.elements
    var i = 0;
    while(elems.hasNext) { arrs(i%numShards) += elems.next; i += 1}
    arrs.toList
  }
  implicit def fakeDistributedIterable[T](it : Iterable[T]):DistributedIterable[T] = new DistributedIterable[T] {
    override def map[U](f : T=>U)  = fakeDistributedIterable(it.map(f));
    override def flatMap[U](f : T=>Iterable[U]) = fakeDistributedIterable(it.flatMap(f));
    override def reduce[U>:T](f : (U,U)=>U)  = it.reduceLeft(f);
    def elements = it.elements;
  }

  private def shardRange (r : scala.Range, numShards : Int) : List[Iterable[Int]]=  {
    val arrs = new ArrayBuffer[Range]
    val n = numShards;
    arrs ++= (for(val i<- 0 until n) yield new Range(r.start + i * r.step,r.end,n * r.step));
    arrs.toList
  }

  private def shardIRange (r : scala.Range.Inclusive, numShards : Int) : List[Iterable[Int]]= {
    val arrs = new ArrayBuffer[Range.Inclusive]
    val n = numShards;
    arrs ++= (for(val i<- 0 until n) yield new Range.Inclusive(r.start + i * r.step ,r.end,n * r.step));
    arrs.toList
  }

  /** 
  * Borrowed from scala source. Just add the annotation tag.
  * Part of the Scala API.
  * original author: @author  Stephane Micheloud
  */
  @serializable
  private[Defaults] class Range(val start: Int, val end: Int, val step: Int) extends RandomAccessSeq.Projection[Int] {
    if (step == 0) throw new Predef.IllegalArgumentException

    /** Create a new range with the start and end values of this range and
    *  a new <code>step</code>.
    */
    def by(step: Int): Range = new Range(start, end, step)

      lazy val length: Int = {
      if (start < end && this.step < 0) 0
      else if (start > end && this.step > 0) 0
      else {
        val base = if (start < end) end - start
        else start - end
        assert(base >= 0)
        val step = if (this.step < 0) -this.step else this.step
        assert(step >= 0)
        base / step + last(base, step)
      }
    }

    protected def last(base: Int, step: Int): Int =
    if (base % step != 0) 1 else 0

    def apply(idx: Int): Int = {
      if (idx < 0 || idx >= length) throw new Predef.IndexOutOfBoundsException
      start + (step * idx)
    }

    /** a <code>Seq.contains</code>, not a <code>Iterator.contains</code>! */
    def contains(x: Int): Boolean = 
      if (step > 0) 
      x >= start && x < end && (((x - start) % step) == 0)
    else 
      x <= start && x > end && (((x - end) % step) == 0)

    def inclusive = new Range.Inclusive(start,end,step)
    }

  private[Defaults] object Range {
    @serializable
    private[Defaults] class Inclusive(start: Int, end: Int, step: Int) extends Range(start, end, step) {
      override def apply(idx: Int): Int = super.apply(idx)
      override protected def last(base: Int, step: Int): Int = 1
      override def by(step: Int): Range = new Inclusive(start, end, step)
    }
  }

}
