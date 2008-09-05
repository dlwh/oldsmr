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

import scala.util.matching._

/**
 * Provides a number of useful utility functions for common maps and reduces.
 */
object Functions {

  // primarily maps
  /**
   * For every string in the input that matches the regex, output (match,1)
   * Ignores key.
   */
  def countMatches[K](r : Regex) : ((K,String))=>Iterator[(String,Int)] = { case (k, s) =>
    for(m <- r.findAllIn(s)) yield (m,1)
  }

  /**
   * Tokenizes the outputs by deliminators as in {@link java.util.StringTokenizer}
   */
  def countTokens[K](delim : String) : ((K,String))=>Iterator[(String,Int)] = { case (k, s) =>
    tokIterator(s,delim) map ( (_,1));
  }

  /**
   * Swaps the key and the value.
   */
  def swap[K,V]( pair : (K,V)) = pair match { case (k,v) => (v,k) }

  // Functions intended for reduce mostly:
  def sum(it : Iterator[Int]) = it.reduceLeft(_+_);
  def sum(it : Iterator[Float]) = it.reduceLeft(_+_);
  def sum(it : Iterator[Double]) = it.reduceLeft(_+_);
  def sum(it : Iterator[Long]) = it.reduceLeft(_+_);


  private def tokIterator(s : String, delim : String) = new Iterator[String] {
    private val mine = new java.util.StringTokenizer(s,delim);
    def hasNext = mine.hasMoreTokens;
    def next = mine.nextToken;
  }
}
