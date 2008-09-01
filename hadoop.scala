import smr.hadoop._;
import org.apache.hadoop.fs.Path;

val h = Hadoop(Array(""),new Path("output"));
println(h.distribute(new Range(0,1000,1),3) reduce ( (x:Int,y:Int)=>x+y));
