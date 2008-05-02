import edu.stanford.nlp.smr._;
import edu.stanford.nlp.smr.defaults._;

val x = new ActorDistributor(3);
println(x.distribute(1 to 100).map(2 * _).reduce(_+_))
println((1 to 100).map(2 * _).reduceLeft(_+_))
