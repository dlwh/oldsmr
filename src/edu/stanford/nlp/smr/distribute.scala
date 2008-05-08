package edu.stanford.nlp.smr;
import scala.actors.Actor;
import scala.actors.Exit;
import scala.actors.Actor._;
import scala.collection.mutable.ArrayBuffer;
import scala.collection._;
import scala.actors.remote.RemoteActor._;
import scala.actors.remote._;
import edu.stanford.nlp.smr.TransActor._;

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
  def gather[T,U](job : JobID, f: T=>U, gather : Actor) :Unit;
  // gets rid of it:
  def remove(job : JobID) : Unit;

  def close() {}
}

sealed case class NumShards(n : Int);

private object Priv {

  // Messages to the scheduler from the disributor
  sealed case class SchedMsg;
  case class Shard[T](it : Iterable[T], shard : Iterable[T]=>List[Iterable[T]]) extends SchedMsg;
  case class Sched(in : JobID, f : Any=>Any) extends SchedMsg;
  case class Get[T,U](job : JobID, f : T => U, gather : Actor) extends SchedMsg;
  case class Remove[U](job : JobID) extends SchedMsg;
  case class AddWorker[U](a : Actor) extends SchedMsg;

  sealed case class WorkerMsg;
  case class Do(id : JobID, f : Any=>Any, out : JobID) extends WorkerMsg;
  case class Retrieve[T,U](in : JobID, f : Any=>Any, out : JobID, actor : SerializedActor) extends WorkerMsg;
  case class DoneAdding(id : JobID) extends WorkerMsg;
  case class Reserve(id : JobID, shard : Int) extends WorkerMsg;
  case class Done[U](id : JobID, shard : Int,  result : U) extends WorkerMsg;
  case object Close extends WorkerMsg;

  case class StartGet(out: JobID, numShards : Int, gather : Actor);
  case class Retrieved(out : JobID, shard : Int, result : Any);
}
import Priv._;

trait DistributedIterable[+T] extends Iterable[T] {
  override def map[B](f : T=>B) : DistributedIterable[B] = null;
  def reduce[B >: T](f : (B,B)=>B) : B;
}

class ActorDistributor(numWorkers : Int, port : Int) extends Distributor {
  override def distribute[T] (it : Iterable[T])
      (implicit myShard : Iterable[T]=>List[Iterable[T]]) : DistributedIterable[T]  = new InternalIterable[T] {
        protected lazy val id : JobID = shard(it)(myShard);
        protected lazy val scheduler = ActorDistributor.this;
      };

  // pushes data onto the grid
  def shard[T](it : Iterable[T])(implicit myShard : Iterable[T]=>List[Iterable[T]]) = (scheduler !?Shard(it,myShard)).asInstanceOf[JobID];
  // runs a task on some data on the grid
  def schedule[T,U](id : JobID, f: T=>U) = (scheduler !? Sched(id,f.asInstanceOf[Any=>Any])).asInstanceOf[JobID];
  // gets it back using some function. Returns immediately. expect output from gather
  def gather[T,U](job : JobID, f: T=>U, gather : Actor) :Unit = (scheduler ! Get(job,f,gather));
  // gets rid of it:
  def remove(job : JobID) : Unit = (scheduler ! Remove(job));

  def addWorker(w : Actor) : Unit = (scheduler ! AddWorker(w));

  override def close = {
    scheduler ! Exit(self,'close);
    workers.foreach(_ ! Close);
  }
  // private stuff:
  classLoader = this.getClass.getClassLoader;

  private val accumulator = transActor(port,'accumulator) {
    val gatherers = mutable.Map[JobID,Actor]();
    val shardsLeft = mutable.Map[JobID,Int]();
    loop {
      react {
        case StartGet(out, numShards, gather) =>
          gatherers(out) = gather;
          shardsLeft(out) = numShards;
          reply{ None}
        case Retrieved(out, shard, result)=>
          gatherers(out) ! Some((shard,result));
          shardsLeft(out) -= 1;
          if(shardsLeft(out) == 0) {
            gatherers(out) ! None;
            shardsLeft -= out;
            gatherers -= out;
          }
      }
    }
  }

  // central dispatcher for ActorDistributor
  private val scheduler = actor {
    val numShards = mutable.Map[JobID,Int]();
    var nextJob : JobID =0
    def getNextJob() = {
      val job = nextJob;
      nextJob +=1; 
      job;
    }
    loop { 
      react {
        case scala.actors.Exit(_,_) => exit();
        case Shard(it,shard)=> 
          val job = getNextJob();
          actor { 
            react {
              case _ =>
              val shards = shard(it)
              numShards += (job -> shards.length);
              shards.zipWithIndex.foreach {
                x => workers(x._2 % workers.length) ! Done(job,x._2,x._1)
              }
              workers.foreach { _ ! DoneAdding(job) }
              reply { job }
            }
          }.forward(None)
        case Sched(in,f)=> 
          val job = getNextJob();
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
        case Get(in,f,gatherer)=>
          val out = getNextJob();
          accumulator !? StartGet(out,numShards(in),gatherer);
          workers.foreach{ _ ! Retrieve(in,f.asInstanceOf[Any=>Any],out,accumulator)}
        case AddWorker(a)=> workers += a; 
        
        case Remove(id) => workers.foreach{ _ ! Remove(id)}
      }
    }
  }

  private val workers =  new ArrayBuffer[Actor];
  for (val i <- List.range(0,numWorkers))
    workers += Worker();
}

trait InternalIterable[T] extends DistributedIterable[T] {
  protected val id : JobID;
  protected val scheduler : Distributor;
  import InternalIterable._;

  def elements = {
    handleGather[T,Iterable[T],Iterable[T]](this,identity[Iterable[T]]).toList.sort(_._1 < _._1).map(_._2.projection).reduceLeft(_ append _).elements
  }

  override def map[U](f : T=>U) : DistributedIterable[U] = handleMap(this,f);
  override def flatMap[U](f : T=>Iterable[U]) : DistributedIterable[U] = handleFlatMap(this,f);
  override def filter(f : T=>Boolean) : DistributedIterable[T] = handleFilter(this,f);
  override def reduce[B >: T](f : (B,B)=>B) : B = handleReduce(this,f)

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
      react {
        case 'start => 
        val replyTo = Actor.sender;
        loop {
          react{
            case Some(x) => b += x.asInstanceOf[(Int,U)];
            case None => replyTo ! b ; exit();
          }
        }
      }
    }
    self.scheduler.gather(self.id,f, recv);
    (recv !? 'start).asInstanceOf[ArrayBuffer[(Int,U)]];
  }

  private def handleMap[T,U](self : InternalIterable[T], f : T=>U) = {
    new InternalIterable[U] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.schedule(self.id,Util.fMap(f));
    }
  }
  private def handleFlatMap[T,U](self : InternalIterable[T], f : T=>Iterable[U]) = {
    new InternalIterable[U] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.schedule(self.id,Util.fFlatMap(f));
    }
  }
  private def handleFilter[T](self : InternalIterable[T], f : T=>Boolean) = {
    new InternalIterable[T] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.schedule(self.id,Util.fFilter(f));
    }
  }

  private def handleReduce[T,B>:T](self : InternalIterable[T], f : (B,B)=>B) =  {
    val b = handleGather[T,Iterable[T],Option[B]](self,(x : Iterable[B])=> if (x.isEmpty) None else Some(x.reduceLeft(f)))
    b.filter(None!=).map( (x : (Int,Option[B])) => x._2.get).reduceLeft(f);
  }
}
