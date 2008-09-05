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
    loop {
      react {
        case Do(id,s,f) => 
        actual_worker ! Do(id,s,f);
        case Done(id,s,r)=> 
        accumulators.getOrElseUpdate(id,Worker.accumulator()) ! Done(id,s,r);
        case DoneAdding(id) => 
        accumulators.getOrElseUpdate(id,Worker.accumulator()) ! DoneAdding(id);
        case Retrieve(id,f) => 
        accumulators.getOrElseUpdate(id,Worker.accumulator()) ! Retrieve(id,f);
        case Reserve(id,shard) => 
        accumulators.getOrElseUpdate(id,Worker.accumulator()) ! Add(shard);
        case Remove(id) => 
        val a = accumulators.get(id)
        accumulators -= id;
        a.map(_.exit());
      }
    }
  }

}

object Worker {
  // intra worker communication:
  private case class Add(shard : Int); 

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
        case Retrieve(id,f) => 
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
          active += s //todo, signal if doneAdding was called.
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
          case  Do(id,s,f) => 
          manager ! Done(id,s,f()) 
        }
      }
    }
}
