package edu.stanford.nlp.smr;
import scala.actors._;
import scala.actors.remote.RemoteActor._;
import scala.actors.remote.Node;
import java.net.InetAddress;

/**
 * Trait to help enable serialization of actors.
 * Should only be used with actors, but whatever.
 * 
 * @author(dlwh)
 */
trait TransActor extends Actor {
  val port : Int;
  val sym : Symbol;
}

sealed case class SerializedActor(node : Node, sym : Symbol);

object TransActor {
  def transActor(port_ : Int, sym_ : Symbol)( body: =>Unit) = new Actor with TransActor {
  val port = port_;
  val sym = sym_;
    start();
    override def act() {
      alive(port);
      register(sym,Actor.self);
      body
    }
  }

  implicit def TransActorToSerializedActor(a : TransActor) : SerializedActor=  {
    SerializedActor(new Node(InetAddress.getLocalHost.getHostName(),a.port),a.sym);
  }

  /**
    * Converts the serialized version of remote actors into a proxy. Note that this is not a
    * TransActor, so it cannot be sent down the wire.
    */
  implicit def SerializedActorToActor(a :SerializedActor) : Actor  = select(a.node,a.sym);

}

