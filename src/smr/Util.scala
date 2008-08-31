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
import scala.actors.Actor._;
import scala.actors.Actor;

object Util {
  def identity[T] = new SerFunction1[T,T] {
    def apply(x : T)  = x;
  };
  def fMap[T,U](f : T=>U) = new SerFunction1[Iterable[T],Iterable[U]] {
    def apply(x : Iterable[T]) = x.map(f);
  };

  def fFlatMap[T,U](f : T=>Iterable[U])  = new SerFunction1[Iterable[T],Iterable[U]] {
    def apply(x : Iterable[T]) = x.flatMap(f);
  };

  def fFilter[T](f : T=>Boolean) = new SerFunction1[Iterable[T],Iterable[T]] { 
    def apply(x : Iterable[T]) = x.filter(f);
  };

  // g(f(x))
  def andThen[A,B,C](f: A=>B, g:B=>C) = new SerFunction1[A,C] {
   def apply(a : A) = g(f(a));
  }

  def freePort() : Int = {
    val server = new java.net.ServerSocket(0);
    val port = server.getLocalPort();
    server.close();
    return port;
  }

  /**
  * Iterator that reacts to get the next element.
  * Used internally to
  */
  class ActorIterator[T] extends Iterator[T] {
    def hasNext() = !nulled && (cache match {
      case Some(x) => true;
      case _ =>
      (receiver !? Poll) match {
        case None => nulled = true; receiver ! Close; false;
        case opt @ Some(x) => cache = opt.asInstanceOf[Option[T]]; true;
      }
    })
    def next() = {hasNext(); val x = cache.get; cache = None; x}

    val receiver = actor {
      loop {
        react {
          case Poll => reply {Actor.?}
          case Close => exit();
        }
      }
    }
    private var nulled = false;
    private var cache : Option[T] = None;
    private case class Poll;
    private case class Close;
  }

  def iteratorFromProducer[T](p : ()=>Option[T]) = new Iterator[T] {
    private var nxt : Option[T] = None;
    private var hasN = true;

    private def readNext() = p() match {
      case o @ Some(t) => nxt = o;
      case None=>nxt=None; hasN = false;
    }

    def hasNext = hasN && (nxt match {
      case Some(t) => true;
      case None => readNext(); hasN;
    })

    def next : T = nxt match {
      case Some(t) => nxt=None; t;
      case None=> readNext(); if(hasN) next else throw new NoSuchElementException();
    }
  }

}
