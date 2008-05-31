/*
* Copyright (c) 2008, David Hall
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*
* THIS SOFTWARE IS PROVIDED BY DAVID HALL ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL DAVID HALL BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package smr;
import scala.actors.Actor;
import scala.actors.Exit;
import scala.actors.Actor._;
import scala.collection.mutable.ArrayBuffer;
import scala.collection._;
import scala.actors.remote.RemoteActor._;
import scala.actors.remote._;
import smr.TransActor._;

object Distributor {
// Every job needs an id:
type JobID = Int;
}
import Distributor._;

/**
 * Trait for defining objects that can automatically distribute tasks to perform on iterables.
 * @author dlwh
 */
trait Distributor {
  /**
  * Generates a DistributedIterable based on the sharding function. Shards from the list are 
  * automatically divvied out to the workers.
  */
  def distribute[T] (it : Iterable[T])(implicit shard : Iterable[T]=>List[Iterable[T]]) : DistributedIterable[T];

  /**
   * Low level operation: should generally not be used, but made public for completeness. 
   * Given a U, automatically shard it out using the shard function to workers.
   * @return a handle to the shards
   */
  def shard[U,V](it : U)(implicit myShard : U=>List[V]) : JobID;

  /**
   * Low level operation: should generally not be used, but made public for completeness. 
   * Convert all sharded elements to U's. More or less a map operation.
   * @return a handle to the changed shards.
   */
  def schedule[T,U](id : JobID, f: T=>U) : JobID;

  /**
   * Low level operation: should generally not be used, but made public for completeness. 
   * Retreive all shards, first applying f to each one. Sent as Some(t) to the Actor.
   * When finished, None is sent.
   * @return a handle to the changed shards.
   */
  def gather[T,U](job : JobID, f: T=>U, gather : Actor) :Unit;

  /**
   * Low level operation: should generally not be used, but made public for completeness. 
   * Delete all shards with this id.
   * @return a handle to the changed shards.
   */
  def remove(job : JobID) : Unit;

  /**
   * Close the distributor and all workers.
   */
  def close() {}
}

private object Priv {

  // Messages to the scheduler from the disributor
  sealed case class SchedMsg;
  case class Shard[U,V](it : U, shard : U=>List[V]) extends SchedMsg;
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
  override def flatMap[U](f : T=>Iterable[U]) : DistributedIterable[U] = null;
  override def filter(f : T=>Boolean) : DistributedIterable[T] = null;
  /**
   * Sadly, both versions of reduce in the Scala libs are not fully associative,
   * which is required for a parallel reduce. This version of reduce demands 
   * that the operators are associative.
   */
  def reduce[B >: T](f : (B,B)=>B) : B;
}

/**
 * Class most users will use. Example use:
 * <pre>
 * val dist = new ActorDistributor(4,4000);
 * dist.distribute(myIterable).map(f).reduce(g);
 * </pre>
 */
class ActorDistributor(numWorkers : Int, port : Int) extends Distributor {
  override def distribute[T] (it : Iterable[T])
    (implicit myShard : Iterable[T]=>List[Iterable[T]]) : DistributedIterable[T]  = new InternalIterable[T] {
        protected lazy val id : JobID = shard(it)(myShard);
        protected lazy val scheduler = ActorDistributor.this;
      };

  // pushes data onto the grid
  def shard[U,V](it : U)(implicit myShard : U=>List[V]) = (scheduler !?Shard(it,myShard)).asInstanceOf[JobID];
  // runs a task on some data on the grid
  def schedule[T,U](id : JobID, f: T=>U) = (scheduler !? Sched(id,f.asInstanceOf[Any=>Any])).asInstanceOf[JobID];
  // gets it back using some function. Returns immediately. expect output from gather
  def gather[T,U](job : JobID, f: T=>U, gather : Actor) :Unit = (scheduler ! Get(job,f,gather));
  // gets rid of it:
  def remove(job : JobID) : Unit = (scheduler ! Remove(job));

  /**
   * Adds a (possibly remote) Worker to the workers list. 
   */
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

private[smr] trait InternalIterable[T] extends DistributedIterable[T] {
  protected val id : JobID;
  protected val scheduler : Distributor;
  import InternalIterable._;

  def elements = {
    handleGather(this,identity[Iterable[T]]).toList.sort(_._1 < _._1).map(_._2.projection).reduceLeft(_ append _).elements
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


/**
 * This object wouldn't exist, except that scala closures pass in the this pointer
 * even if you don't use any state. Objects don't have that restriction.
 */
private[smr] object InternalIterable {
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
