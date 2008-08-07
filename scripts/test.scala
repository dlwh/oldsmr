import smr._;
import smr.defaults._;

//scala.actors.Debug.level = 10;
val x = new ActorDistributor(4,9010);
println("Go!");
println((1 to 10000000).map(BigInt(_)).reduceLeft(_+_))
println(x.distribute(1 to 10000000).map(BigInt(_)).reduce(_+_))
println(x.distribute( (1 to 100).toList).map(6*).map(BigInt(_)).reduce(_+_))
x.close();
System.exit(0);
