import edu.stanford.nlp.smr._;
import edu.stanford.nlp.smr.defaults._;

//scala.actors.Debug.level = 10;
val x = new ActorDistributor(3,9010);
println("Go!");
println((1 to 10000000).map(BigInt(_)).reduceLeft(_+_))
println(x.distribute(1 to 10000000).map(BigInt(_)).reduce(_+_))
println(x.distribute(1 to 100).map(6*).map(BigInt(_)).reduce(_+_))
x.close();
