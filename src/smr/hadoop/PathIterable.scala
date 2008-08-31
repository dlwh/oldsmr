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

class PathIterable[+T](h : Hadoop, val paths : Array[Path]) extends Iterable[T] {
  def elements = {
    if(paths.length == 0) 
      new Iterator[T] { 
        def hasNext = false;
        def next = throw new IllegalArgumentException("No elements were found!")
      }
    else (for(p <- paths;
            rdr = new SequenceFile.Reader(p.getFileSystem(h.conf),p,h.conf);
            valType = rdr.getValueClass().asSubclass(classOf[Writable]))
      yield new Iterator[T] {
        private var hasN = true;
        private var v : Option[Writable] = None;
        private def readNext() {
          val realV = valType.newInstance();
          val k = new Hadoop.DefaultKeyWritable;
          try {
            hasN = rdr.next(k,realV);
            if(!hasN) rdr.close();
            else v = Some(realV);
          } catch {
            case e => rdr.close();
          }
        }

        def hasNext() = v match {
          case None => readNext(); hasN
          case Some(v) => true;
        }
        def next() = {
          val nx = Magic.wireToReal(v.get).asInstanceOf[T];
          v = None;
          nx;
        }
      }.asInstanceOf[Iterator[T]]).reduceLeft(_++_);
  }

  import PathIterable._;
  import Hadoop._;
  def reduce[T](f: (T,T)=>T) : T = { 
    val output = h.runMapReduce(paths, new CollectorMapper[T,T](reduceFun[T](f)), new RealReduce(f));
    val path = output(0);

    val result = new SequenceFile.Reader(path.getFileSystem(h.conf),path,h.conf);
    val v = result.getValueClass.asSubclass(classOf[Writable]).newInstance();
    val k = result.getKeyClass.asSubclass(classOf[Writable]).newInstance();
    result.next(k,v);
    result.close();
    Magic.wireToReal(v).asInstanceOf[T];
  }

 // override def map[U](f : T=>U) : Iterable[U] = new ProjectedIterable[U](this,Util.fMap(f));
 // override def flatMap[U](f : T=>Iterable[U]) : Iterable[U] = new ProjectedIterable[U](this,Util.fFlatMap(f));
 // override def filter[U](f : T=>Boolean) : Iterable[T] = new ProjectedIterable[U](this,Util.fFilter(f));
}

private[smr] object PathIterable {
  def reduceFun[T](f : (T,T)=>T) = { (it:Iterator[T])=>
    Iterator.single(it.reduceLeft(f));
  }
}
