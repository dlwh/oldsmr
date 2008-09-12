//
// an smr hub keeps track of a bunch of workers
//
// dramage 2008
//

package smr.actors;

import scala.actors.Actor
import scala.actors.OutputChannel
import scala.actors.Actor._
import scala.actors.remote.Node
import scala.actors.remote.RemoteActor._

/**
 * A hub stores a list of active Workers.
 */
class Hub(port : Int) extends Actor {
  import Hub._
  def this() = this(Util.freePort);

  private var workers : List[(Symbol,String,Int)] = Nil
  
  println("Hub: registering as hub on port " + port)

  start()
  
  override def act() {
    alive(port)
    register('hub, self)
    
    loop {
      println("Hub: ready")
      react {
        case HubRegister(name,machine,port) =>
          println("Hub: registering "+(name,machine,port))
          workers = (name,machine,port) :: workers
        case r:HubListRequest =>
          println("Hub: listing to "+sender)
          reply { HubListResponse(workers) } 
        case x:Any =>
          println("Hub: other message "+x)
      }
    }
  }
  
  println("access");
  scala.actors.remote.RemoteActor.classLoader = classOf[Hub].getClassLoader 
}

/**
 * Provides a mechanism to get a distributor that works across the registered
 * classes.
 */
object Hub {
  case class HubRegister(name : Symbol, machine : String, port : Int)
  case class HubListRequest
  case class HubListResponse(workers : List[(Symbol,String,Int)])

  import Util._;
  
  def apply(machine : String, port : Int) : OutputChannel[Any] = {
    scala.actors.remote.RemoteActor.classLoader = classOf[Hub].getClassLoader
    return select(Node(machine,port),'hub)
  }

  def workers(machine : String, port : Int) = {
    (Hub(machine,port) ! Hub.HubListRequest());
    val workers : List[(Symbol,String,Int)] = self.?.asInstanceOf[HubListResponse].workers;
    workers map ((x:(Symbol,String,Int)) => select(Node(x._2,x._3),x._1)) 
  }
  
  def distributor(machine : String, port : Int) : ActorDistributor = {
    val distributor = new ActorDistributor(0, freePort())
    workers(machine,port) foreach distributor.addWorker
    distributor
  }
  
  def list(machine : String, port : Int) {
    (Hub(machine,port) ! Hub.HubListRequest());
    val workers : List[(Symbol,String,Int)] = self.?.asInstanceOf[HubListResponse].workers
    
    workers foreach println
  }
}

/**
 * Spawns a new hub
 */
object SpawnHub {
  def main(argv : Array[String]) {
    scala.actors.remote.RemoteActor.classLoader = classOf[Hub].getClassLoader
    scala.actors.Debug.level = 10;
    
    new Hub()
  }
}

/**
 * Spawns a worker thread, registering it with the hub named in argv.
 * TODO: error message argv != Array("host","port").
 */
object SpawnWorker {
  def main(argv : Array[String]) {
    scala.actors.Debug.level = 10;
    if(argv.length < 2) {
      println("Syntax: SpawnWorker <host> <port> [numWorkers] [classLoader class]");
    }

    scala.actors.remote.RemoteActor.classLoader = if(argv.length > 3)  Class.forName(argv(3)).getClassLoader else classOf[Hub].getClassLoader

    val numWorkers = if(argv.length > 2) java.lang.Integer.parseInt(argv(2)) else Runtime.getRuntime.availableProcessors;
    
    val hub  = select(Node(argv(0),java.lang.Integer.parseInt(argv(1))), 'hub)

    val host = java.net.InetAddress.getLocalHost().getHostName()

    for(i <- 1 to numWorkers) {
      var port = Util.freePort()
      val worker = Worker(port,'worker);
      hub ! Hub.HubRegister('worker, host, port)
    }

    println("Worker: starting")
  }
}

