import edu.stanford.nlp.smr._;
import edu.stanford.nlp.smr.defaults._;

val x = new ActorDistributor(3);
println("Go!");
println((1 to 10000000).map(BigInt(_)).reduceLeft(_+_))
println(x.distribute(1 to 10000000).map(BigInt(_)).reduce(_+_))
