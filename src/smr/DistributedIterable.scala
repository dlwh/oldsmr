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

trait DistributedIterable[+T] extends Iterable[T] {
  override def map[B](f : T=>B) : DistributedIterable[B] = null;
  override def flatMap[U](f : T=>Iterable[U]) : DistributedIterable[U] = null;
  override def filter(f : T=>Boolean) : DistributedIterable[T] = null;
  /**
   * Sadly, both versions of reduce in the Scala libs are not fully associative,
   * which is required for a parallel reduce. This version of reduce demands 
   * that the operators are associative.
   */
  def reduce[B >: T](f : (B,B)=>B) : B;

  /**
   * Can do a map and a reduce in a single step. Useful for large data sets.
   */
  def mapReduce[U,R>:U](m : T=>U)(r : (R,R)=>R) : R = this.map(m).reduce(r);

  /**
  * for each element, reshard the data by group(t)'s hashcode and create a new 
  * Iterable with those elements.
  */
  def groupBy[U](group : T=>U) : DistributedIterable[(U,Iterable[T])];

  /**
  * Removes all copies of the elements.
  */
  def distinct() : DistributedIterable[T];

  /**
   * Returns a "lazy" DistributedIterable that does not invoke the supplied 
   * operation until another non-lazy operation is applied.
   * Because this whole library is designed for large memory tasks,
   * using a lazyMap is occasionally useful.
   * 
   * A lazyMap followed by a reduce is the same as a mapReduce.
   *
   */
  def lazyMap[U](f : T=>U) :DistributedIterable[U] ={
    val parent = this;
    new DistributedIterable[U] {
      def elements = parent.elements.map(f);
      override def map[C](g : U=>C) = parent.map(Util.andThen(f,g));
      override def flatMap[C](g: U=>Iterable[C]) = parent.flatMap(Util.andThen(f,g));
      override def filter(g: U=>Boolean) = parent.map(f).filter(g);
      override def reduce[C >:U](g : (C,C) =>C) : C= parent.mapReduce[U,C](f)(g)
      override def mapReduce[B,C>:B](m : U=>B)(r : (C,C)=>C) = parent.mapReduce[B,C](Util.andThen(f,m))(r);
      override def lazyMap[C](g : U=>C) : DistributedIterable[C] = parent.lazyMap(Util.andThen(f,g));
      // TODO: better partition
      override def groupBy[V]( grp : U=>V) = parent.map(f).groupBy(grp);
      override def distinct() = parent.map(f).distinct;
    }
  }
}


