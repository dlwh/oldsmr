package edu.stanford.nlp.smr;
import scala.actors.Actor;
import scala.actors.Actor._;
import scala.collection.mutable.ArrayBuffer;
import scala.collection._;
import scala.actors.remote.RemoteActor._;
import scala.actors.remote._;

object Public {
// Every job needs an id:
type JobID = Int;
}
import Public._;

trait Distributor {
  def distribute[T] (it : Iterable[T])(implicit shard : Iterable[T]=>List[Iterable[T]]) : DistributedIterable[T];

  // pushes data onto the grid
  def shard[T](it : Iterable[T])(implicit myShard : Iterable[T]=>List[Iterable[T]]) : JobID;
  // runs a task on some data on the grid
  def schedule[T,U](id : JobID, f: T=>U) : JobID;
  // gets it back using some function
  def gather[T,U](job : JobID, f: T=>U, gather : (Int,U)=>Unit) :Unit;
  // gets rid of it:
  def remove(job : JobID) : Unit;

  def close() {}
}

sealed case class NumShards(n : Int);


private object Priv {

  // Messages to the scheduler from the disributor
  private[smr] sealed case class SchedMsg;
  private[smr] case class Shard[T](it : Iterable[T], shard : Iterable[T]=>List[Iterable[T]]) extends SchedMsg;
  private[smr] case class Sched(in : JobID, f : Any=>Any) extends SchedMsg;
  private[smr] case class Get[T,U](job : JobID, f : T => U, gather : (Int,U)=>Unit) extends SchedMsg;
  private[smr] case class Remove[U](job : JobID) extends SchedMsg;

  private[smr] sealed case class WorkerMsg;
  private[smr] case class Do(id : JobID, f : Any=>Any, out : JobID) extends WorkerMsg;
  private[smr] case class Retrieve[T,U](id : JobID, f : Any=>Any, actor : Actor) extends WorkerMsg;
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
        protected lazy val scheduler = ActorDistributor.this;
      };

  // pushes data onto the grid
  def shard[T](it : Iterable[T])(implicit myShard : Iterable[T]=>List[Iterable[T]]) = (scheduler !?Shard(it,myShard)).asInstanceOf[JobID];
  // runs a task on some data on the grid
  def schedule[T,U](id : JobID, f: T=>U) = (scheduler !? Sched(id,f.asInstanceOf[Any=>Any])).asInstanceOf[JobID];
  // gets it back using some function
  def gather[T,U](job : JobID, f: T=>U, gather : (Int,U)=>Unit) :Unit = (scheduler !? Get(job,f,gather));
  // gets rid of it:
  def remove(job : JobID) : Unit = (scheduler ! Remove(job));

  override def close = {
    scheduler.exit();
    workers.foreach(_.exit());
  }

  private val scheduler = actor {
    // helper method to please the type system
    def handle_get[T,U](id : JobID, numShards : Int, f : T=>U, gather : (Int,U)=>Unit) {
      actor { 
        react {
          case _ =>
          val s = Actor.self
          workers.foreach{ _ ! Retrieve(id,f.asInstanceOf[Any=>Any],s)}
          for(val i <- 1 to numShards) receive {
            case x : Tuple2[_,_] =>  gather(x._1.asInstanceOf[Int],x._2.asInstanceOf[U]) 
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
            workers.foreach { a =>  
              a ! Do(in, f, job)
            }
            reply { job }
          }
        }.forward(None)
        case Get(id,f,gather)=> handle_get(id,numShards.get(id).get,f,gather) // please the type system
        case Remove(id) => workers.foreach{ _ ! Remove(id)}
      }
    }
  }

  private val workers = {
    for (val i <- List.range(0,numWorkers))
      yield Worker();
  }
}

trait InternalIterable[T] extends DistributedIterable[T] {
  protected val id : JobID;
  protected val scheduler : Distributor;
  import InternalIterable._;

  def elements = {
    val b = InternalIterable.handleGather[T,Iterable[T],Iterable[T]](this,identity[Iterable[T]]);
    b.toList.sort(_._1 < _._1).map(_._2.projection).reduceLeft(_ append _).elements
  }

  override def map[U](f : T=>U) : DistributedIterable[U] = {
    handleMap(this,f);
  }
  override def flatMap[U](f : T=>Iterable[U]) : DistributedIterable[U] = {
    handleFlatMap(this,f);
  }

  override def filter(f : T=>Boolean) : DistributedIterable[T] = {
    handleFilter(this,f);
  }

  override def reduce[B >: T](f : (B,B)=>B) : B = {
    val b = handleGather[T,Iterable[T],B](this,(y :Iterable[T]) =>  y.reduceLeft(f) )
    b.toList.sort(_._1 < _._1).map( (x : (Int,B)) => x._2).reduceLeft(f);
  }
  override protected def finalize() {
    try {
      scheduler.remove(id);
    } finally {
      super.finalize();
    }
  }
}

object InternalIterable {
  private def handleGather[T,C,U](self : InternalIterable[T], f : C=>U) = {
    val recv = actor { 
      val b = new ArrayBuffer[(Int,U)];
      loop {
        react{
          case x : (Int,U) => b += x;
          case None => reply { b}
        }
      }
    }
    self.scheduler.gather(self.id,f, { (x :Int, y : U) => recv ! (x,y) });
    (recv !? None).asInstanceOf[ArrayBuffer[(Int,U)]];
  }

  private def handleMap[T,U](self : InternalIterable[T], f : T=>U) = {
    new InternalIterable[U] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.schedule(self.id,(x : Iterable[T]) => x.map(f))
    }
  }
  private def handleFlatMap[T,U](self : InternalIterable[T], f : T=>Iterable[U]) = {
    new InternalIterable[U] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.schedule(self.id,(x : Iterable[T]) => x.flatMap(f))
    }
  }
  private def handleFilter[T](self : InternalIterable[T], f : T=>Boolean) = {
    new InternalIterable[T] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.schedule(self.id,(x : Iterable[T]) => x.filter(f))
    }
  }
}
