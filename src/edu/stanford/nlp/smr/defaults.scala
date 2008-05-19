package edu.stanford.nlp.smr;
import scala.collection.mutable.ArrayBuffer;
object defaults {
  implicit val defaultNumShards = NumShards(5);

  /**
   * Implicit shard function that provides a reasonable default in most cases. Special treatment for
   * Ranges and for Collections
   */
  implicit def  shard[T] (it : Iterable[T])(implicit numShards : NumShards) : List[Iterable[T]] = it match {
    case x : scala.Range.Inclusive => shardIRange(x)(numShards).asInstanceOf[List[Iterable[T]]];
    case x : scala.Range=> shardRange(x)(numShards).asInstanceOf[List[Iterable[T]]];
    case x : Collection[_] => 
    if(x.size < numShards.n) {
     List(x) 
    } else {
      val sz = x.size / numShards.n;
      val arrs = new ArrayBuffer[Iterable[T]]
      arrs ++= (for(val i <- 0 until numShards.n ) yield x.drop(sz * i).take(sz));
      arrs.toList
    }
    case _ =>
    val arrs = new ArrayBuffer[ArrayBuffer[T]]
    arrs ++= (for(val i <- 1 to numShards.n) yield new ArrayBuffer[T]);
    val elems = it.elements
    var i = 0;
    while(elems.hasNext) { arrs(i%numShards.n) += elems.next; i += 1}
    arrs.toList
  }

   
  implicit def shardRange (r : scala.Range)(implicit numShards : NumShards) : List[Iterable[Int]]=  {
    val arrs = new ArrayBuffer[Range]
    val n = numShards.n;
    arrs ++= (for(val i<- 0 until n) yield new Range(r.start + i * r.step,r.end,n * r.step));
    arrs.toList
  }
  implicit def shardIRange (r : scala.Range.Inclusive)(implicit numShards : NumShards) : List[Iterable[Int]]= {
    val arrs = new ArrayBuffer[Range.Inclusive]
    val n = numShards.n;
    arrs ++= (for(val i<- 0 until n) yield new Range.Inclusive(r.start + i * r.step ,r.end,n * r.step));
    arrs.toList
  }


/** 
 * Borrowed from scala source. Just add the annotation tag.
 * Part of the Scala API.
 * original author: @author  Stephane Micheloud
 */
@serializable
class Range(val start: Int, val end: Int, val step: Int) extends RandomAccessSeq.Projection[Int] {
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

object Range {
  @serializable
  class Inclusive(start: Int, end: Int, step: Int) extends Range(start, end, step) {
    override def apply(idx: Int): Int = super.apply(idx)
    override protected def last(base: Int, step: Int): Int = 1
    override def by(step: Int): Range = new Inclusive(start, end, step)
  }
}

}
