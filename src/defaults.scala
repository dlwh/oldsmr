package edu.stanford.nlp.smr;
import scala.collection.mutable.ArrayBuffer;
object defaults {
  implicit val defaultNumShards = NumShards(5);

  implicit def  shard[T] (it : Iterable[T])(implicit numShards : NumShards) : List[Iterable[T]] = it match {
    case x : Range.Inclusive => shardIRange(x.asInstanceOf[Range.Inclusive])(numShards).asInstanceOf[List[Iterable[T]]];
    case x : Range=> shardRange(x.asInstanceOf[Range])(numShards).asInstanceOf[List[Iterable[T]]];
    case _ =>
    val arrs = new ArrayBuffer[ArrayBuffer[T]]
    arrs ++= (for(val i <- 1 to numShards.n) yield new ArrayBuffer[T]);
    val elems = it.elements
    var i = 0;
    while(elems.hasNext) { arrs(i%numShards.n) += elems.next; i += 1}
    arrs.toList
  }

  implicit def shardRange (r : Range)(implicit numShards : NumShards) : List[Iterable[Int]]=  {
    val arrs = new ArrayBuffer[Range]
    val n = numShards.n;
    arrs ++= (for(val i<- 0 until n) yield new Range(r.start + i * r.step,r.end,n * r.step));
    arrs.toList
  }
  implicit def shardIRange (r : Range.Inclusive)(implicit numShards : NumShards) : List[Iterable[Int]]= {
    val arrs = new ArrayBuffer[Range.Inclusive]
    val n = numShards.n;
    arrs ++= (for(val i<- 0 until n) yield new Range.Inclusive(r.start + i * r.step ,r.end,n * r.step));
    arrs.toList
  }
}
