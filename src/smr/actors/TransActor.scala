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
package smr.actors;
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
  implicit def SerializedActorToActor(a :SerializedActor) : OutputChannel[Any]  = select(a.node,a.sym);

}

