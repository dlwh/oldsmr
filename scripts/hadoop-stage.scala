import smr.hadoop._;
import org.apache.hadoop.fs.Path;

val h = Hadoop(Array(""),new Path("output"));
val words = for( (off,line) <- h.loadLines(new Path("build.xml"));
              word <- line.split(" "))
            yield(word,1);

val counts = words.reduce{ (word,it) =>
  (word,it.reduceLeft(_+_));
}.asStage("counts");

counts.elements foreach println;
val counts2 = words.reduce{ (word,it) =>
  (word,it.reduceLeft(_+_));
}.asStage("counts");

counts2.elements foreach println;
