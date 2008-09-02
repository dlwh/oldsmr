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
package smr.hadoop;
import smr._;

import org.apache.hadoop.io._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.fs._;
import org.apache.hadoop.util._;

@serializable
trait Reduce[-K1,-V1,+K2,+V2] {
  def reduce(key : K1, it: Iterator[V1]): Iterator[(K2,V2)];
}

import Hadoop._;
class RealReduce[T](f : (T,T)=>T) extends Reduce[DefaultKey,T,DefaultKey,T] { 
  override def reduce(k: DefaultKey, it :Iterator[T]) : Iterator[(DefaultKey,T)] = {
    Iterator.single((k,it.reduceLeft(f)));
  }
}

/**
* Reduce that takes (K,[V]) and returns (car[V],K)
 */
class KeyToValReduce[K,V] extends Reduce[K,V,V,K] {
  override def reduce(k : K, it :  Iterator[V]) : Iterator[(V,K)] = {
    it.take(1).map( (_,k));
  }
}

class IdentityReduce[K,V] extends Reduce[K,V,K,V] {
  override def reduce(k : K,  it :Iterator[V]): Iterator[(K,V)] = {
    it.map((k,_));  
  }
}
