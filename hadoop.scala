import smr.hadoop._;
import org.apache.hadoop.fs.Path;

val h = Hadoop(Array(""),new Path("output"));
println(h.distribute(1 to 1000,3) reduce ( (x:Int,y:Int)=>x+y));
