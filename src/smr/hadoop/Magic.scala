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

import java.io._;

import org.apache.hadoop.io._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.fs._;
import org.apache.hadoop.util._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.filecache._;

import scala.reflect.Manifest;

// You know it's bad when you have a class called magic
private object Magic {
  def wireToReal(t : Writable) :Any = t match {
    case t :Text => t.toString;
    case arr : ArrayWritable => arr.get().map(wireToReal);
    case t => try {
      t.asInstanceOf[{def get():Any;}].get();
    } catch {
      case e => t;
    }
  }

  implicit def realToWire(t : Any):Writable = t match {
    case t : Writable => t;
    case t : Int => new IntWritable(t);
    case t : Long => new LongWritable(t);
    //case t : Byte => new ByteWritable(t);
    case t : Float => new FloatWritable(t);
    //case t : Double => new DoubleWritable(t);
    case t : Boolean => new BooleanWritable(t);
    case t : String => new Text(t);
    case t : Array[Byte] => new BytesWritable(t);
    case x : AnyRef if x.getClass.isArray => { 
      val t = x.asInstanceOf[Array[Any]];
      if(t.length == 0) new AnyWritable(t);
      else { 
        val mapped = t.map(realToWire); 
        val classes = mapped.map(_.getClass);
        if(classes.forall(classes(0)==_)) { 
          // can only use ArrayWritable if all Writables are the same.
          new ArrayWritable(classes(0),mapped);
        } else {
          // fall back on AnyWritable
          val mapped = t.map(new AnyWritable[Any](_).asInstanceOf[Writable]); 
          new ArrayWritable(classOf[AnyWritable[_]],mapped);
        }
      }
    }
    case _ => new AnyWritable(t);
  }

  private val CInt = classOf[Int];
  private val CLong = classOf[Long];
  private val CByte = classOf[Byte];
  private val CDouble = classOf[Double];
  private val CFloat = classOf[Float];
  private val CBoolean = classOf[Boolean];
  private val CString = classOf[String];
  private val CArrayByte = classOf[Array[Byte]];
  private val CArray = classOf[Array[_]];

  def mkManifest[T](c:Class[T]) = new Manifest[T] {
    def erasure = c;
  }

  private val CWritable = mkManifest(classOf[Writable]);

  def classToWritableClass[T](c: Class[T]):Class[Writable] = c match {
    case c if mkManifest(c) <:< CWritable => c.asInstanceOf[Class[Writable]];
    case CInt => classOf[IntWritable].asInstanceOf[Class[Writable]];
    case CLong => classOf[LongWritable].asInstanceOf[Class[Writable]];
   // case CByte => classOf[ByteWritable].asInstanceOf[Class[Writable]];
    //case CDouble => classOf[DoubleWritable].asInstanceOf[Class[Writable]];
    case CFloat => classOf[FloatWritable].asInstanceOf[Class[Writable]];
    case CBoolean => classOf[BooleanWritable].asInstanceOf[Class[Writable]];
    case CString => classOf[Text].asInstanceOf[Class[Writable]];
    case CArrayByte => classOf[BytesWritable].asInstanceOf[Class[Writable]];
    case CArray => classOf[ArrayWritable].asInstanceOf[Class[Writable]];
    case _ => classOf[AnyWritable[_]].asInstanceOf[Class[Writable]];
  }

}
