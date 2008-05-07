import edu.stanford.nlp.smr._
import edu.stanford.nlp.smr.defaults._

//scala.actors.Debug.level = 10;
val y = new ActorDistributor(0,9000)
val w = Worker(9000,'worker);
y.addWorker(w);
println(y.distribute( (1 to 10).toList).map(_ * 2).reduce(_+_));
y.close();
System.exit(0);
