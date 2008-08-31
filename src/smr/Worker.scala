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
import scala.actors.Actor._;
import scala.actors.Exit;
import scala.collection.mutable.ArrayBuffer;
import scala.collection._;
import smr.TransActor._;
import scala.actors.remote.RemoteActor._;
import scala.actors.remote.Node;

import Distributor._;
import Priv._;

class Worker(port : Int, sym : Symbol) extends Actor {
  import Worker._;

  def this() = this(Util.freePort,'worker);

  start();

  def act() {
    alive(port);
    register(sym,Actor.self);

    trapExit = true;

    val actual_worker = new RealWorker();
    val accumulators = mutable.Map[JobID,Accumulator]();
    def getAcc(id : JobID) ={ 
      accumulators.getOrElseUpdate(id,new Accumulator(id));
    }

    loop {
      react {
        case msg @ Do(in,f,out) => 
          //Debug.info(msg + "");
          val outAcc = getAcc(out);
          getAcc(in).forwardShardNums(outAcc);
          getAcc(in).addShardListener {  case (shard,data) =>
            actual_worker.enqueue { x:Unit =>
              val outData = f(data);
              outAcc.completeShard(shard,outData);
            }
          }
        case InPlaceDo(in,f) =>
        getAcc(in).addShardListener { case (s,data) =>
          actual_worker enqueue { x:Unit =>
            f(data);
          }
        }
        case GetOutputActor(isLocal, out, shard, retr) => 
        def getOutputActor[U,V](retr : Iterator[U]=>V) {
          val actorIterator = new Util.ActorIterator[U];
          val a = Actor.actor {
            getAcc(out).completeShard(shard,retr(actorIterator));
          }
          getAcc(out).reserveShard(shard);
          val actor = transActor(port,Symbol(":output-" + out + "-"+shard)) {
            Actor.loop {
              Actor.react {
                case msg@ Some(x) => actorIterator.receiver ! msg; 
                case None => actorIterator.receiver ! None; exit();
              }
            }
          }
          if(isLocal) {
            reply { (Some(actor),TransActorToSerializedActor(actor))}
          } else {
            reply { (None,TransActorToSerializedActor(actor))}
          }
        }
        getOutputActor(retr);
        case Done(id,s,r)=>getAcc(id).completeShard(s,r);
        case Reserve(id,shard) => getAcc(id).reserveShard(shard);
        case DoneAdding(id) => getAcc(id).doneReserving();
        case rtr @ Retrieve(id,f,out,a) => 
          val realActor = a match {
            case Right(a) => SerializedActorToActor(a);
            case Left(a) => a
          }
          //Debug.info(rtr + "");
          // Push it off to the accumulator, have it forward things to the job runner
          getAcc(id).addShardListener{ case (shard,data) =>
            actual_worker.enqueue { x :Unit =>
              realActor ! Retrieved(out,shard,f(data));
            }
          }
        case Close=>
          Debug.info("Worker " + self + " shutting down");
          actual_worker.close();
          accumulators.values.foreach(_.close());
          exit();
        case Remove(id) => 
          val a = accumulators.get(id);
          accumulators -= id;
          Debug.info("Worker " + self + " removing job " + id);
          a.foreach( _.close());
        case x =>
          Debug.error( "Wrong input to worker! " + x);
      }
    }
  }

}

object Worker {
  def apply()  = new Worker();
  def apply(port : Int, sym : Symbol) = new Worker(port,sym);

  /*
  def setClassLoaderFromClass(c : Class[_]) {
    scala.actors.remote.RemoteActor.classLoader = classLoaderToUse
    classLoaderToUse = c.getClassLoader();
  }

  private var classLoaderToUse = this.getClass.getClassLoader();
*/
  private class Accumulator(id : JobID) {
    private case class Forward(out : Accumulator); 
    private case class Add(shard : Int); 
    private case class Retr(f : ((Int,Any))=>Unit); 

    def forwardShardNums(out : Accumulator) = inner ! Forward(out);
    def completeShard(shard : Int, data : Any) = inner ! Done(id,shard,data);
    def addShardListener(f :  ((Int,Any))=>Unit) = inner ! Retr(f);
    def reserveShard(shard : Int) = inner ! Add(shard);
    def doneReserving() = inner ! DoneAdding(0);
    def close() = inner ! Close

    private val inner : Actor =actor {
      val active = mutable.Set[Int]();
      val done = mutable.Map[Int,Any]();
      val awaiting = new ArrayBuffer[((Int,Any))=>Unit]();
      val waitingForDoneReservation = new ArrayBuffer[Unit=>Unit]();
      var doneAdding = false;
      var shouldExit = false;

      def checkFinished() {
        if(doneAdding && active.size == 0) {
          awaiting.foreach{f => done.foreach(f)}
          awaiting.clear();
          if(shouldExit) exit();
        }
      }

      loop {
        react {
          case Add(s) => 
            if(doneAdding) Debug.warning("Got a late add");
            if(!done.contains(s)) active += s
          case Close => 
            if(awaiting.isEmpty)  {
              done.clear();
              waitingForDoneReservation.clear();
              exit();
            }
            shouldExit = true;
          case Forward(out) =>
            val f = { x: Unit => 
              active.foreach { sh =>  out.reserveShard(sh)}
              done.keys.foreach { sh =>  out.reserveShard(sh)}
              out.doneReserving();
            }
            if(doneAdding) f();
            else  waitingForDoneReservation += f;

          case Retr(f) => 
            if(doneAdding && active.size==0) {
              done.foreach(f);
            } else {
              awaiting += f;
            }
          case msg @ DoneAdding(dbg) => 
            doneAdding = true;
            waitingForDoneReservation foreach { f => f()}
            waitingForDoneReservation.clear();
            checkFinished();

          case Done(i,s,r) => 
            active -= s; 
            done += (s->r);
            checkFinished();
          case x => Debug.error( "Wrong input to accumulator!" + x);
        }
      }
    }
  }

  private class RealWorker {
    private case class Enqueue(f : Unit=>Unit);
    def enqueue(f : Unit=>Unit) = inner ! Enqueue(f);
    def close() = inner ! Exit(Actor.self,'closed);
      
    private val inner = actor { 
      loop {
        react {
          case Exit(_,_) => exit();
          case Enqueue(f) => try {
            f();
          } catch {
            case x =>
            // todo: better error reporting
            x.printStackTrace();
          }
          case x => Debug.error( "Wrong input to realWorker!" + x);
        }
      }
    }
  }
}
