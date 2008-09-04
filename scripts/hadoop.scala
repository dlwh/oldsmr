import smr.hadoop._;
import org.apache.hadoop.fs.Path;

val h = Hadoop(Array(""),new Path("output"));
println(h.distribute(1 to 1000,3) reduce ( _+_));
println(h.distribute(1 to 1000,3) map (2*)  reduce ( _+_));

val words = for( (off,line) <- h.loadLines(new Path("build.xml"));
              word <- line.split(" "))
            yield(word,1);

val counts = words.hreduce{ (word,it) =>
  Iterator.single((word,it.reduceLeft(_+_)));
}

counts.elements foreach println;
