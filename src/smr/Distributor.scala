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
import scala.actors.OutputChannel;
import scala.actors.Exit;
import scala.actors.Actor._;
import scala.collection.mutable.ArrayBuffer;
import scala.collection._;
import scala.actors.remote.RemoteActor._;
import scala.actors.remote._;
import scala.reflect.Manifest;
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
  def distribute[T] (it : Iterable[T])(implicit shard : (Iterable[T],Int)=>List[Iterable[T]]) : DistributedIterable[T];

  /**
   * Low level operation: should generally not be used, but made public for completeness. 
   * Given a U, automatically shard it out using the shard function to workers.
   * @return a handle to the shards
   */
  def shard[U,V](it : U)(implicit myShard : (U,Int)=>List[V]) : JobID;

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
  * Take input shards of type T and creates a new set of shards U, which are processed.
  * This can support a Google-style MapReduce
  * @return a handle to the changed shards.
  */
  def groupBy[T,U,V](job : JobID, f: (T,((Int,U)=>Unit))=>Unit, received: Iterator[U]=>V): JobID;

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
  case class Shard[U,V](it : U, shard : (U,Int)=>List[V]) extends SchedMsg;
  case class GroupBy[T,U,V](job: JobID, f: (T,((Int,U)=>Unit))=>Unit, received: Iterator[U]=>V) extends SchedMsg;
  case class Sched(in : JobID, f : Any=>Any) extends SchedMsg;
  case class Get[T,U](job : JobID, f : T => U, gather : Actor) extends SchedMsg;
  case class Remove[U](job : JobID) extends SchedMsg;
  case class AddWorker[U](a :OutputChannel[Any]) extends SchedMsg;

  sealed case class WorkerMsg;
  case class Do(id : JobID, f : Any=>Any, out : JobID) extends WorkerMsg;
  case class InPlaceDo(id : JobID, f : Any=>Unit) extends WorkerMsg;
  case class Retrieve[T,U](in : JobID, f : Any=>Any, out : JobID, actor : Either[Actor,SerializedActor]) extends WorkerMsg;
  case class GetOutputActor[U,V](isLocal : Boolean, out : JobID, shard : Int, process : Iterator[U]=>V) extends WorkerMsg;
  case class DoneAdding(id : JobID) extends WorkerMsg;
  case class Reserve(id : JobID, shard : Int) extends WorkerMsg;
  case class Done[U](id : JobID, shard : Int,  result : U) extends WorkerMsg;
  case object Close extends WorkerMsg;

  case class StartGet(out: JobID, numShards : Int, gather : Actor);
  case class Retrieved(out : JobID, shard : Int, result : Any);
}
import Priv._;

object Debug extends scala.actors.Debug("smr:: ") {
  level = 4;
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
    (implicit myShard : (Iterable[T],Int)=>List[Iterable[T]]) : DistributedIterable[T]  = new InternalIterable[T] {
        protected lazy val id : JobID = shard(it)(myShard);
        protected lazy val scheduler = ActorDistributor.this;
      };

  // pushes data onto the grid
  def shard[U,V](it : U)(implicit myShard : (U,Int)=>List[V]) = (scheduler !?Shard(it,myShard)).asInstanceOf[JobID];
  // runs a task on some data on the grid
  def schedule[T,U](id : JobID, f: T=>U) = (scheduler !? Sched(id,f.asInstanceOf[Any=>Any])).asInstanceOf[JobID];
  // gets it back using some function. Returns immediately. expect output from gather
  def gather[T,U](job : JobID, f: T=>U, gather : Actor) :Unit = (scheduler ! Get(job,f,gather));
  // gets rid of it:
  def remove(job : JobID) : Unit = (scheduler ! Remove(job));

  def groupBy[T,U,V](job : JobID, f: (T,((Int,U)=>Unit))=>Unit, received: Iterator[U]=>V) =
    (scheduler !? GroupBy(job,f,received)).asInstanceOf[JobID];
  /**
   * Adds a (possibly remote) Worker to the workers list. 
   */
  def addWorker(w :OutputChannel[Any]) : Unit = (scheduler ! AddWorker(w));

  override def close = {
    scheduler ! Exit(self,'close);
    workers.foreach(_._2 ! Close);
  }

  private val gatherer = actor {
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

  // Accumulator is a remote actor, so it just acts a middle man for gatherer.
  // Otherwise, potentially large amounts of data would get serialized in the gather closure for no reason.
  private val remoteAccumulator = transActor(port,'accumulator) {
    loop {
      react {
        case x => gatherer ! x
      }
    }
  }

  private val localAccumulator = actor {
    loop {
      react {
        case x => gatherer ! x
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
          val shards = shard(it,workers.length)
          numShards += (job -> shards.length);
          shards.zipWithIndex.foreach {
            x => 
            Debug.info( "sending shard " + x._2 + " to Worker " + x._2 %workers.length);
            workers(x._2 % workers.length)._2 ! Done(job,x._2,x._1)
          }
          workers.foreach { _._2 ! DoneAdding(job) }
          reply { job }
        case Sched(in,f)=> 
          val job = getNextJob();
          val oldNumShards = numShards(in);
          numShards += (job->oldNumShards);
          Debug.info( "Running " + f.getClass.getName() + " on job " + in + "'s output as job " + job);
          workers.foreach { a =>  
            a._2 ! Do(in, f, job)
          }
          reply { job }
        case GroupBy(in, f, r) =>
          // get type inference...
          def handleGroupBy[T,U,V](in : JobID, f : ( (T,(Int,U)=>Unit)=>Unit), r : Iterator[U]=>V) {
            val out = getNextJob();
            val oldNumShards = numShards(in);
            numShards += (out->oldNumShards);
            // set up forwarding actors for the hashed outputs
            val outActors = getOutActors(out, oldNumShards, r);
            for( (isLocal,w) <- workers) {
              w ! DoneAdding(out);
            }

            val localActors = outActors.map(getLocalActors);
            def localOut(x : Any) {
              def output(idx : Int, u : U) {
                localActors(idx%localActors.length) ! Some(u);
              }
              f(x.asInstanceOf[T],output);
              localAccumulator ! Retrieved(out,1,None);
            }

            val remoteActors = outActors.map(getRemoteActors);
            def remoteOut(x :Any) {
              def output(idx : Int, u : U) {
                remoteActors(idx%remoteActors.length) ! Some(u);
              }
              f(x.asInstanceOf[T],output);
              remoteAccumulator ! Retrieved(out,1,None);
            }
            val rendevezous = actor { 
              loop {
                react {
                  case None =>
                    localActors.foreach{ _ ! None};
                  case _ => // don't care about results, just want to know when i'm done.
                }
              }
            }
            gatherer ! StartGet(out, oldNumShards, rendevezous);
            for( (isLocal,w) <- workers) {
              w ! InPlaceDo(in,if(!isLocal) remoteOut else localOut);
            }
            reply {out};
          }
          handleGroupBy(in,f,r);
        case Get(in,f,gather)=>
          val out = getNextJob();
          Debug.info( "Getting job "  + in  + " with function " + f.getClass.getName() + " as job id " + out);
          gatherer !? StartGet(out,numShards(in),gather);
          workers.foreach{ a => a._2 ! Retrieve(in,f.asInstanceOf[Any=>Any],out,if(a._1) Left(localAccumulator) else Right(remoteAccumulator))}
        case AddWorker(a)=> 
          Debug.info("Added a worker.");
          workers += new Tuple2(false,a); // TODO:improve 
        
        case Remove(id) =>
          Debug.info("Master removing job " + id);
          workers.foreach{ _._2 ! Remove(id)};
          numShards -= id;
      }
    }
  }
  private def getOutActors[U,V](out : JobID, numShards : Int, r : Iterator[U]=>V) = {
    for(i <- 0 until numShards;
        (isLocal,w) =  workers(i%numShards)) {
      w ! GetOutputActor(isLocal, out, i, r);
    }
    val buff = new ArrayBuffer[(Option[Actor],SerializedActor)];
    for( i <- 1 to numShards) {
      buff += (Actor.?).asInstanceOf[(Option[Actor],SerializedActor)];
    }
    buff.toSeq;
  }

  private def getLocalActors(a : (Option[Actor],SerializedActor)):OutputChannel[Any] = a._1 match {
    case Some(a) => a;
    case None => a._2
  }
  private def getRemoteActors(a : (Option[Actor],SerializedActor)): OutputChannel[Any] = a._2;
  // boolean says i'm local and don't need to serialize things
  private val workers =  new ArrayBuffer[(Boolean,OutputChannel[Any])];
  for (val i <- List.range(0,numWorkers))
    workers += new Tuple2(true,Worker());
}

private[smr] trait InternalIterable[T] extends DistributedIterable[T] {
  protected val id : JobID;
  protected val scheduler : Distributor;
  import InternalIterable._;

  def elements = {
    val list : List[(Int,Iterable[T])] = handleGather(this,Util.identity[Iterable[T]]).toList;
    list.sort(_._1 < _._1).map(_._2.projection).reduceLeft(_ append _).elements
  }

  def map[U](f : T=>U)(implicit mU : Manifest[U]) : DistributedIterable[U] = handleMap(this,f);
  def flatMap[U](f : T=>Iterable[U]) (implicit mU : Manifest[U]): DistributedIterable[U] = handleFlatMap(this,f);
  def filter(f : T=>Boolean) : DistributedIterable[T] = handleFilter(this,f);
  def reduce[B >: T](f : (B,B)=>B) : B = handleReduce(this,f)
  override def mapReduce[U,B >: U](m : T=>U)(r : (B,B)=>B)(implicit mU:Manifest[U]) = {
    handleMapReduce(this,m,r);
  }
  def groupBy[U](group: T=>U):DistributedIterable[(U,Seq[T])] = handleGroupBy(this,group);
  def distinct() = handleDistinct(this);
  def force = this;

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
  private def handleGather[T,C,U](self : InternalIterable[T], f : SerFunction1[C,U]) = {
    val recv = actor { 
      val b = new ArrayBuffer[(Int,U)];
      react {
        case 'start => 
        val replyTo = Actor.sender;
        loop {
          react{
            case Some(x) => 
              b += x.asInstanceOf[(Int,U)]; 
              Debug.info("Got shard " + x.asInstanceOf[(Int,U)]._1);
            case None => replyTo ! b ; exit();
          }
        }
      }
    }
    self.scheduler.gather(self.id, f, recv);
    (recv !? 'start).asInstanceOf[ArrayBuffer[(Int,U)]];
  }

  private def handleMap[T,U](self : InternalIterable[T], f : T=>U) = {
    new InternalIterable[U] {
      protected val scheduler = self.scheduler;
      Debug.info("Map with " + f.getClass.getName);
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
    val b = handleGather[T,Iterable[T],Option[B]](self,new SerFunction1[Iterable[T],Option[B]]{
        def apply(x : Iterable[T])= if (x.isEmpty) None else Some(x.reduceLeft(f));
    });
    b.filter(None!=).map{ (x : (Int,Option[B])) => x._2.get}.reduceLeft(f);
  }

  private def handleMapReduce[T,U,B>:U](self :InternalIterable[T], m : T=>U, r : (B,B)=>B) = {
    Debug.info("MapReduce with " + m.getClass.getName + " and reduce " + r.getClass.getName);
  
    val doMapReduce = new SerFunction1[Iterable[T],Option[B]] {
      def apply(x : Iterable[T])  = {
        if (x.isEmpty) None 
        else {
          var elems = x.elements;
          var acc : B = m(elems.next);;
          while(elems.hasNext) acc= r(acc,m(elems.next));
          Some(acc);
        }
      }
    }
    val b = handleGather[T,Iterable[T],Option[B]](self,doMapReduce);
    b.filter(None!=).map{ (x : (Int,Option[B])) => x._2.get}.reduceLeft(r);
  }

  private def handleGroupBy[T,U](self : InternalIterable[T], group : T=>U) = {
    val innerGroupBy = { (it : Iterable[T],out : (Int,(U,T)) =>Unit) =>
      for(x <- it) {
        val ARBITRARY_PRIME=47;
        val u = group(x);
        out(u.hashCode+ARBITRARY_PRIME,(u,x));
      }
    }

    val receiver = { (it: Iterator[(U,T)]) => 
      val map = scala.collection.mutable.Map[U,ArrayBuffer[T]]();
      for( (u,t) <- it) {
        map.getOrElseUpdate(u,new ArrayBuffer[T]) += t;
      }
      map.toSeq;
    }
    new InternalIterable[(U,Seq[T])] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.groupBy(self.id,innerGroupBy, receiver);
    }
  }

  private def handleDistinct[T](self : InternalIterable[T]) = {
    val innerGroupBy = { (it : Iterable[T],out : (Int,T) =>Unit) =>
      for(x <- it) {
        val ARBITRARY_PRIME=47;
        out(x.hashCode+ARBITRARY_PRIME,x);
      }
    }

    val receiver = { (it: Iterator[T]) => 
      val set = scala.collection.mutable.Set[T]() ++ it;
      set.toSeq;
    }
    new InternalIterable[T] {
      protected val scheduler = self.scheduler;
      protected val id = scheduler.groupBy(self.id,innerGroupBy, receiver);
    }
  }

}
