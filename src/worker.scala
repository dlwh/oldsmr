package edu.stanford.nlp.smr;
import scala.actors.Actor;
import scala.actors.Actor._;
import scala.collection.mutable.ArrayBuffer;
import scala.collection._;

import Public._;
import Priv._;

class Worker extends Actor {
  import Worker._;


  def act() {
    val actual_worker = realWorker(Actor.self);
    val accumulators = mutable.Map[JobID,Actor]();
    val self = Actor.self;
    def getAcc(id : JobID) = accumulators.getOrElseUpdate(id,Worker.accumulator());
    loop {
      react {
        case Do(in,f,out) => 
         getAcc(in) ! Retr(in,{
            x : (Int,Any) => 
              getAcc(out) ! Add(x._1);
              actual_worker ! { () =>self ! Done(out,x._1,f(x._2))};
          });
        case Done(id,s,r)=> 
        getAcc(id) ! Done(id,s,r);
        case DoneAdding(id) => 
        getAcc(id) ! DoneAdding(id);
        case Retrieve(id,f,a) => 
        getAcc(id) ! Retr(id,{
            x : (Int,Any) => actual_worker ! { () => a ! ((x._1,f(x._2)))}; 
          });
        case Reserve(id,shard) => 
        getAcc(id) ! Add(shard);
        case Remove(id) => 
        val a = accumulators.get(id)
        accumulators -= id;
        a.map(_.exit()); // remove it if it exists
      }
    }
  }

}

object Worker {
  // intra worker communication:
  private case class Add(shard : Int); 

  private case class Retr(id: JobID, f : ((Int,Any))=>Unit); 

  def apply() = {
    val w = new Worker();
    w.start();
    w;
  }
  private def accumulator() = actor {
    val active = mutable.Set[Int]();
    val done = mutable.Map[Int,Any]();
    val awaiting = new ArrayBuffer[Actor]();
    var doneAdding = false;
    loop {
      react {
        case Retr(id,f) => 
          val a =  actor {
            react {
              case DoneAdding(_) => 
              done.foreach(f)
            } 
          }
          if(doneAdding && active.size == 0) a !  DoneAdding(0);
          else awaiting += a
        case DoneAdding(_) => 
          doneAdding = true;
          if(active.size == 0) {
            awaiting.foreach(_ ! DoneAdding(0));
          }
        case Add(s) => 
          if( !(done contains s)) active += s //todo, signal if doneAdding was called.
        case Done(x,s,r) => 
          active -= s; 
          done += (s->r);
          if(doneAdding && active.size == 0) {
            awaiting.foreach(_ ! DoneAdding(0));
          }
      }
    }
  }
  def realWorker(manager :Actor) = actor { 
      loop {
        react {
          case f : (Unit=>Unit) => f();
        }
      }
    }
}
