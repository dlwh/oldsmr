import edu.stanford.nlp.smr._;
import edu.stanford.nlp.smr.defaults._;
import org.scalacheck._;
import Arbitrary._;
import Prop._;

val x = new ActorDistributor(3,9010);
val prop_SumReduceIdentity = property { (l : List[Int]) => l.size == 0 || x.distribute(l).reduce(_+_) == l.reduceLeft(_+_) }
Test.check(prop_SumReduceIdentity);
x.close();
System.exit(0);
