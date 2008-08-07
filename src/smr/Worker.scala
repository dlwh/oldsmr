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

class Worker extends Actor {
  import Worker._;

  def act() {
    trapExit = true;
    val actual_worker = realWorker(Actor.self);
    val accumulators = mutable.Map[JobID,Actor]();
    def getAcc(id : JobID) ={ 
      accumulators.getOrElseUpdate(id,Worker.accumulator(id));
    }
    loop {
      react {
        case Enliven(port,sym)=>
          alive(port);
          register(sym,Actor.self);
          reply{None}
        case Do(in,f,out) => 
         getAcc(in) ! Forward(getAcc(out));
         val outA = getAcc(out);
         getAcc(in) ! Retr(in,{
            x : (Int,Any) => 
              actual_worker ! { () => 
              outA ! Done(out,x._1,f(x._2))};
          });
        case Done(id,s,r)=> 
          getAcc(id) ! Done(id,s,r);
          Debug.info( "Worker storing job " + id + " shard " + s);
        case DoneAdding(id) => 
        //println("external " + id);
        getAcc(id) !? DoneAdding(id);
        case Retrieve(id,f,out,a) => 
        val a2 = a match {
          case Right(a) =>println("Darn!"); SerializedActorToActor(a);
          case Left(a) => a
        }

        getAcc(id) ! Retr(id,{
            x : (Int,Any) =>actual_worker ! { () => a2 ! Retrieved(out,x._1,f(x._2)); }; 
          });
        case Reserve(id,shard) => 
        getAcc(id) !? Add(shard);
        case Close=>
          Debug.info("Worker " + self + " shutting down");
          actual_worker ! Exit(self,'close);
          accumulators.values.map(_ ! Exit(self,'close));
          exit();
        case Remove(id) => 
        val a = accumulators.get(id)
          accumulators -= id;
          Debug.info("Worker " + self + " Removing job " + id);
          a.map( _ ! Exit(self,'remove));
        case x =>
          Debug.error( "Wrong input to worker!" + x);
      }
    }
  }

  // private stuff:
  classLoader = this.getClass.getClassLoader;
}

object Worker {
  // intra worker communication:
  private case class Add(shard : Int); 
  private case class Forward(out : Actor); 
  private case class Enliven(port : Int,sym : Symbol);

  private case class Retr(id: JobID, f : ((Int,Any))=>Unit); 

  def apply()  : Worker = {
    val w = new Worker();
    w.start();
    w;
  }

  def apply(port : Int, sym : Symbol) : Worker = {
    val w = apply();
    w ! Enliven(port,sym);
    w
  }

  private def accumulator(id : JobID) = actor {
    val active = mutable.Set[Int]();
    val done = mutable.Map[Int,Any]();
    val awaiting = new ArrayBuffer[Actor]();
    var doneAdding = false;
    loop {
      react {
        case Exit(a,f) => done.clear(); exit();
        case Forward(out) =>
          //println("forward" + id + " to " + out);
          if(doneAdding) {
            active.foreach { x =>  out !? Add(x)}
            done.keys.foreach{ x => out !? Add(x)}
            out !? DoneAdding(1);
            //println("fast done" + id);
          } else {
            val a =  actor {
              //println("waiting on signal");
              react {
                case DoneAdding(_) => 
                  done.keys.foreach{ out ! Add(_)}
                  out ! DoneAdding(1);
                  //println("slow");
              } 
            }
            if(doneAdding && active.size == 0) a !  DoneAdding(0);
            else awaiting += a
          }
        case Retr(id,f) => 
          //println(Retr(id,f));
          val a =  actor {
            react {
              case DoneAdding(_) => 
              //println("Retr go!" + id);
              done.foreach(f)
            } 
          }
          if(doneAdding && active.size == 0) a !  DoneAdding(0);
          else awaiting += a
        case DoneAdding(_) => 
          //println("doneA" + id + "from" + Actor.sender);
          doneAdding = true;
          if(active.size == 0) {
            awaiting.foreach(_ ! DoneAdding(0));
          }
          reply{None}
        case Add(s) => 
          //println("adding" + s + " to " + id + "from " + Actor.sender);
          if(doneAdding) println("Warning: " + id + " got Add for shard " + s + "after doneAdding");
          if( !(done contains s)) active += s
          reply(None);
        case Done(x,s,r) => 
          //println("Done" + x + " " + s);
          active -= s; 
          done += (s->r);
          if(doneAdding && active.size == 0) {
            awaiting.foreach(_ ! DoneAdding(0));
          }
        case x =>
          Debug.error( "Wrong input to worker!" + x);
      }
    }
  }
  def realWorker(manager :Actor) = actor { 
      loop {
        react {
          case Exit(_,_) => exit();
          case f : Function0[_] => try {  f(); } catch { case x => x.printStackTrace();}
          case x => Debug.error( "Wrong input to realWorker!" + x);
        }
      }
    }
}
