import edu.stanford.nlp.smr._
import edu.stanford.nlp.smr.defaults._

val y = new ActorDistributor(0,1019)
val w = Worker.startRemoteActor('worker,9000);
y.addRemoteWorker("localhost",9000,'worker);
y.distribute( (1 to 10).toList).map(_ * 2).reduce(_+_);
