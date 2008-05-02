import edu.stanford.nlp.smr._;
import edu.stanford.nlp.smr.defaults._;

val x = new ActorDistributor(3);
val r = 1 to 100
val id = x.schedule(r,(x : Iterable[Int]) => x.map(x => 3))
println(id)
val b = new scala.collection.mutable.ArrayBuffer[(Int,Iterable[Int])]
x.gather(id,(x : Int, y : Iterable[Int]) => b += (x, y))
b.toList.sort( _._1 < _._1).reduceLeft(_._2 ++ _._2)

x.distribute(1 to 100).map(2 * _).foreach(println)
