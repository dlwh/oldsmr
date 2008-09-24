package smr.examples.hadoop;
import smr.hadoop._;
import org.apache.hadoop.fs._;

object Basic {
  def main(args :Array[String]) {

    val (h,remainingArgs) = Hadoop.fromArgs(args.drop(1).force, args(0), new Path("output"));
    println(h.distribute(1 to 1000,3) reduce ( _+_));
    println(h.distribute(1 to 1000,3) map (2*)  reduce ( _+_));

    val words = 
      for( (off,line) <- h.loadLines(new Path("file:///u/dlwh/src/scalanlp/build.xml"));
          word <- line.split(" "))
        yield(word,1);

    val counts = words.reduce{ (word,it) =>
      (word,it.reduceLeft(_+_));
    }

    counts.elements foreach println;
    counts.elements foreach println;
  }
}
