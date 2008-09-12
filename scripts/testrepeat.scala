import smr._;
import smr.defaults._;

//scala.actors.Debug.level = 10;
val x = new ActorDistributor(3,9010);
val c = x.distribute(1 to 10000);
println(c.map(2*).reduce(_+_));
println(c.map(2*).reduce(_+_));
println(c.reduce(_+_));
x.close();
System.exit(0);
