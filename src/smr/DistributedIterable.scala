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

import scala.reflect.Manifest;

/**
 * A variant of {@link scala.Iterable} that's more amenable to distribution.
 * The design goal is to make these lazy by default, but the ActorDistributor 
 * returns eager iterables at the moment.
 */
trait DistributedIterable[+T] { self =>
  def elements : Iterator[T];
  def map[B](f : T=>B)(implicit m : Manifest[B]): DistributedIterable[B];
  def flatMap[U](f : T=>Iterable[U])(implicit m: Manifest[U]) : DistributedIterable[U];
  def filter(f : T=>Boolean) : DistributedIterable[T];

  /**
   * Process any computations that have been cached and return a new
   * DistributedIterable with those results.
   */
  def force() : DistributedIterable[T];

  /**
   * Sadly, both versions of reduce in the Scala libs are not fully associative,
   * which is required for a parallel reduce. This version of reduce demands 
   * that the operators are associative.
   */
  def reduce[B >: T](f : (B,B)=>B): B;

  /**
  * for each element, reshard the data by group(t)'s hashcode and create a new 
  * Iterable with those elements.
  */
  //def groupBy[U](group : T=>U) : DistributedIterable[(U,Iterable[T])];

  /**
  * Removes all copies of the elements.
  */
  def distinct() : DistributedIterable[T];

  def toIterable : Iterable[T] = new Iterable[T] {
    def elements = self.elements;
  }

  // compatibility: will be removed soon:
  @deprecated
  def mapReduce[U,B>:U](f : T=>U)(r : (B,B)=>B)(implicit mU:Manifest[U]) = map(f).reduce(r);
  @deprecated
  def lazyMap[U](f : T=>U)(implicit mU:Manifest[U])= map(f);
}


