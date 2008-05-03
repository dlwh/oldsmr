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

object Public {
// Every job needs an id:
type JobID = Int;
}
import Public._;

private object Priv {

  // Messages to the scheduler from the disributor
  private[smr] sealed case class SchedMsg;
  private[smr] case class Shard[T](it : Iterable[T], shard : Iterable[T]=>List[Iterable[T]]) extends SchedMsg;
  private[smr] case class Sched(in : JobID, f : Any=>Any) extends SchedMsg;
  private[smr] case class Get[U](job : JobID, gatherer : (Int,U)=>Unit) extends SchedMsg;
  private[smr] case class Remove[U](job : JobID) extends SchedMsg;

  private[smr] sealed case class WorkerMsg;
  private[smr] case class Do(id : JobID, shard : Int, f : Unit=>Any) extends WorkerMsg;
  private[smr] case class Retrieve(id : JobID, f : ((Int,Any))=>Unit) extends WorkerMsg;
  private[smr] case class DoneAdding(id : JobID) extends WorkerMsg;
  private[smr] case class Reserve(id : JobID, shard : Int) extends WorkerMsg;
  private[smr] case class Done[U](id : JobID, shard : Int,  result : U) extends WorkerMsg;
}
import Priv._;

trait DistributedIterable[+T] extends Iterable[T] {
  override def map[B](f : T=>B) : DistributedIterable[B] = null;
  def reduce[B >: T](f : (B,B)=>B) : B;
}

class ActorDistributor(numWorkers : Int) extends Distributor {
  override def distribute[T] (it : Iterable[T])
      (implicit myShard : Iterable[T]=>List[Iterable[T]]) : DistributedIterable[T]  = new InternalIterable[T] {
        protected lazy val id : JobID = shard(it)(myShard);
      };

  // pushes data onto the grid
  def shard[T](it : Iterable[T])(implicit myShard : Iterable[T]=>List[Iterable[T]]) = (scheduler !?Shard(it,myShard)).asInstanceOf[JobID];
  // runs a task on some data on the grid
  def schedule[T,U](id : JobID, f: Iterable[T]=>U) = (scheduler !? Sched(id,f.asInstanceOf[Any=>Any])).asInstanceOf[JobID];
  // gets it back using some function
  def gather[U](job : JobID, gather: (Int,U)=>Unit) :Unit = (scheduler !? Get(job,gather));
  // gets rid of it:
  def remove(job : JobID) : Unit = (scheduler ! Remove(job));

  private val scheduler = actor {
    // helper method to please the type system
    def handle_get[U](id : JobID, numShards : Int, f : (Int,U)=>Unit) {
      actor { 
        react {
          case _ =>
          val s = Actor.self
          workers.foreach{ _ ! Retrieve(id,{(x : (Int,Any)) => s ! x})}
          for(val i <- 1 to numShards) receive {
            case x : Tuple2[_,_] => f(x._1.asInstanceOf[Int],x._2.asInstanceOf[U]) 
          }
          reply{ None}
        }
      }.forward(None)
    }
    val numShards = mutable.Map[JobID,Int]();
    var nextJob : JobID =0
    loop { 
      react {
        case Shard(it,shard)=> 
        val job = nextJob; nextJob +=1; 
        actor { 
          react {
            case _ =>
            val shards = shard(it)
            numShards += (job -> shards.length);
            shards.zipWithIndex.foreach {
              x => workers(x._2 % numWorkers) ! Done(job,x._2,x._1)
            }
            workers.foreach { _ ! DoneAdding(job) }
            reply { job }
          }
        }.forward(None)
        case Sched(in,f)=> 
        val job = nextJob; nextJob +=1; 
        actor { 
          react {
            case _ =>
            val oldNumShards = numShards.get(in).get;
            numShards += (job -> oldNumShards);
            (0 until oldNumShards).foreach { x => workers(x%numWorkers) ! Reserve(job,x)}
            workers.foreach { a =>  
              a ! DoneAdding(job)
              a ! Retrieve(in, {
                (x : (Int,Any)) => a ! Do(job,x._1,(Unit) => f(x._2))
              })
            }
            reply { job }
          }
        }.forward(None)
        case Get(id,f)=> handle_get(id,numShards.get(id).get,f) // please the type system
        case Remove(id) => workers.foreach{ _ ! Remove(id)}
      }
    }
  }

  private val workers = {
    for (val i <- List.range(0,numWorkers))
      yield Worker();
  }

  trait InternalIterable[T] extends DistributedIterable[T] {
    protected val id : JobID;
    def elements = {
      val b = new scala.collection.mutable.ArrayBuffer[(Int,Iterable[T])]
      gather[Iterable[T]](id,{ (x : Int, y :Iterable[T]) => b+= (x,y) })
      b.toList.sort(_._1 < _._1).map(_._2.projection).reduceLeft(_ append _).elements
    }
    override def map[U](f : T=>U) : DistributedIterable[U] = {
      val outer = id;
      new InternalIterable[U] {
        protected val id = schedule(outer,(x : Iterable[T]) => x.map(f))
      }
    }
    override def flatMap[U](f : T=>Iterable[U]) : DistributedIterable[U] = {
      val outer = id;
      new InternalIterable[U] {
        protected val id = schedule(outer,(x : Iterable[T]) => x.flatMap(f))
      }
    }
    override def filter(f : T=>Boolean) : DistributedIterable[T] = {
      val outer = id;
      new InternalIterable[T] {
        protected val id = schedule(outer,(x : Iterable[T]) => x.filter(f))
      }
    }
    override def reduce[B >: T](f : (B,B)=>B) : B = {
      val b = new scala.collection.mutable.ArrayBuffer[(Int,B)]
      gather[Iterable[T]](id,{ (x : Int, y :Iterable[T]) =>  b += ( x -> y.reduceLeft(f)) })
      b.toList.sort(_._1 < _._1).map( (x : (Int,B)) => x._2).reduceLeft(f);
    }
    override protected def finalize() {
      try {
        remove(id);
      } finally {
        super.finalize();
      }
    }
  }
}
