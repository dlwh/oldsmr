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
import org.apache.hadoop.mapred._;

/**
 * See {@link Implicits$}
 */
trait Implicits {

  implicit def stringToPath(s : String) = new Path(s);
  /**
   * An awful lot of Path operations take the form p.getFileSystem(conf).doFoo(p).
   * This makes some of them look like "p.doFoo"
   */
  implicit def pathToUsefulPath(p : Path)(implicit conf : Configuration) = new UsefulPath(p,conf);
  
  class UsefulPath(p : Path, conf: Configuration) {
    def mkdirs() { p.getFileSystem(conf).mkdirs(p);}
    def createNewFile() = { p.getFileSystem(conf).create(p);}
    def exists() = { p.getFileSystem(conf).exists(p);}
    def listFiles() = {p.getFileSystem(conf).globStatus(new Path(p,"*")).map(_.getPath);}
    def length() = {p.getFileSystem(conf).getLength(p)}
    def deleteOnExit() = {p.getFileSystem(conf).deleteOnExit(p)}
    def delete() = {p.getFileSystem(conf).delete(p)}
  }

  implicit def recordReaderToIterator[K,V](r : RecordReader[K,V]) = new Iterator[(K,V)] {
    def hasNext = kv match {
      case None => readNext(); hasN;
      case Some(_) => true;
    }

    def next = {
      val nx = kv.get
      kv = None;
      nx;
    }

    private def readNext() {
      val v = r.createValue;
      val k = r.createKey;
      try {
        hasN = r.next(k,v);
        if(!hasN) r.close();
        else kv = Some((k,v))
      } catch {
        case e => r.close();
      }
    }

    private var kv :Option[(K,V)] = None;
    private var hasN = true;
  }

  implicit def fromWritable(w : IntWritable)  = w.get();
  implicit def fromWritable(w : LongWritable) = w.get();
  implicit def fromWritable(w : DoubleWritable) = w.get();
  implicit def fromWritable(w : ByteWritable) = w.get();
  implicit def fromWritable(w : FloatWritable) = w.get();
  implicit def fromText(t : Text) = t.toString();
  implicit def fromWritable[T](w : ArrayWritable) = {
    w.get().map(Magic.wireToReal).asInstanceOf[Array[T]]
  }
  def fromWritable[T](w : ObjectWritable) : Any = w.get()

}

/**
 * Provides a number of implicit conversions for using the lower level Hadoop API.
 * Not really necessary if you stick with SMR's magic.
 */ 
object Implicits extends Implicits{
}
