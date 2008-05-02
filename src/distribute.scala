package edu.stanford.nlp.smr;
import scala.actors.Actor;
import scala.actors.Actor._;
import scala.collection.mutable.ArrayBuffer;
import scala.collection._;

trait Distributor {
  def distribute[T] (it : Iterable[T])(implicit shard : Iterable[T]=>List[Iterable[T]]) : DistributedIterable[T];
  def close() {}
}

sealed case class NumShards(n : Int);

object defaults {
  implicit val defaultNumShards = NumShards(5);

  implicit def  shard[T] (it : Iterable[T])(implicit numShards : NumShards) : List[Iterable[T]] = {
    val arrs = new ArrayBuffer[ArrayBuffer[T]]
    arrs ++= (for(val i <- 1 to numShards.n) yield new ArrayBuffer[T]);
    val elems = it.elements
    var i = 0;
    while(elems.hasNext) { arrs(i%numShards.n) += elems.next; i += 1}
    arrs.toList
  }
}

private object Priv {
}
import Priv._;

trait DistributedIterable[+T] extends Iterable[T] {
    def shard : List[Iterable[T]];
}

class ActorDistributor(numWorkers : Int) extends Distributor {
  override def distribute[T] (it : Iterable[T])
      (implicit myShard : Iterable[T]=>List[Iterable[T]]) : DistributedIterable[T]  = new DistributedIterable[T] {
        def elements = it.elements
        def shard = myShard(it);
        override def map[U](f : T => U) : Iterable[U] = new Iterable[U]{
          private val id = schedule(it,(x : Iterable[T]) => x.map(f))(myShard)
          def elements = {
            val b = new scala.collection.mutable.ArrayBuffer[(Int,Iterable[U])]
            gather[Iterable[U]](id,{ (x : Int, y :Iterable[U]) =>
              b+= (x,y)
            })
            b.toList.sort(_._1 < _._1).map(_._2.projection).reduceLeft(_ append _).elements
          }
        }

      };

  def schedule[T,U](it : Iterable[T], f: Iterable[T]=>U)(implicit myShard : Iterable[T]=>List[Iterable[T]]) = (scheduler !? Sched(it,myShard,f)).asInstanceOf[JobID];
  def gather[U](job : JobID, gather: (Int,U)=>Unit) :Unit = (scheduler !? Get(job,gather));

  // Every job needs an id:
  type JobID = Int;

  // Messages to the scheduler from the disributor
  private sealed case class SchedMsg;
  private case class Sched[T,U](it : Iterable[T], shard : Iterable[T]=>List[Iterable[T]], f : Iterable[T]=>U) extends SchedMsg;
  private case class Get[U](job : JobID, gatherer : (Int,U)=>Unit) extends SchedMsg;

  private sealed case class WorkerMsg;
  private case class Do(id : JobID, shard : Int, f : Unit=>Any) extends WorkerMsg;
  private case class Retrieve(id : JobID, f : ((Int,Any))=>Unit) extends WorkerMsg;
  private case class DoneAdding(id : JobID) extends WorkerMsg;


  private val scheduler = actor {
    // helper method to please the type system
    def handle_get[U](id : JobID, numShards : Int, f : (Int,U)=>Unit) {
      actor { 
        react {
          case _ =>
          val s = Actor.self
          workers.foreach{ _ ! Retrieve(id,{(x : (Int,Any)) => s ! x})}
          for(val i <- 1 to numShards) receive {
            case x : (Int,Any) => f(x._1,x._2.asInstanceOf[U]) 
            println(3);
          }
          reply{ None}
        }
      }.forward(None)
    }
    val numShards = mutable.Map[JobID,Int]();
    var nextJob : JobID =0
    loop { 
      react {
        case Sched(it,shard,f)=> 
        val job = nextJob; nextJob +=1; 
        actor { 
          react {
            case _ =>
            val shards = shard(it)
            numShards += (job -> shards.length);
            shards.zipWithIndex.foreach {
              x => workers(x._2 % numWorkers) ! Do(job,x._2, (Unit) => f(x._1))
            }
            workers.foreach { _ ! DoneAdding(job) }
            reply { job }
          }
        }.forward(None)
        case Get(id,f)=> handle_get(id,numShards.get(id).get,f) // please the type system
      }
    }
  }

  // intra worker communication:
  private case class Done(id : JobID, shard : Int,  result : Any);
  private case class Add(shard : Int); 

  private val workers = {
    for (val i <- List.range(0,numWorkers))
      yield actor {
        val accumulators = mutable.Map[JobID,Actor]();
        val manager = Actor.self;
        val actual_worker = actor { 
          loop {
            react {
             case  Do(id,s,f) => 
             manager ! Done(id,s,f()) 
             println("x");
            }
          }
        }
        loop {
          react {
            case Do(id,s,f) => 
              accumulators.getOrElseUpdate(id,accumulator()) ! Add(s); 
              actual_worker ! Do(id,s,f)
            case Done(id,s,r)=> 
              accumulators.getOrElseUpdate(id,accumulator()) ! Done(id,s,r);
            case DoneAdding(id) => 
            println("z");
              accumulators.getOrElseUpdate(id,accumulator()) ! DoneAdding(id);
            case Retrieve(id,f) => 
              accumulators.getOrElseUpdate(id,accumulator()) forward Retrieve(id,f);
          }
        }
      }
  }

  private def accumulator() = actor {
    val active = mutable.Set[Int]();
    val done = mutable.Map[Int,Any]();
    val awaiting = new ArrayBuffer[Actor]();
    var doneAdding = false;
    loop {
      react {
        case Retrieve(id,f) => val a =  actor {
          react {
            case None =>
            println("w1");
            react {
              case DoneAdding(_) => 
              println("w2");
              done.foreach(f)
            } 
          }
        }
        println("w" + " " + doneAdding + " " + Actor.self);
        a ! None
        if(doneAdding) a !  DoneAdding(0);
        awaiting += a
        case DoneAdding(_) => 
        doneAdding = true;
        println("a" + " " + doneAdding + " " + Actor.self);
        if(active.size == 0) {
          awaiting.foreach(_ ! DoneAdding(0));
        }
        case Add(s) => 
          println(Add(s));
          active += s //todo, signal if doneAdding was called.
        case Done(x,s,r) => 
          println(Done(x,s,r));
            println("y " + s);
          // todo: signal if doneAdding was called.
          active -= s; 
          done += (s->r);
          if(doneAdding && active.size == 0) {
            awaiting.foreach(_ ! DoneAdding(0));
          }
      }
    }
  }

  private abstract class InternalIterable[+T](val id : JobID) extends DistributedIterable[T] {
  }
}
