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
import org.apache.hadoop.filecache._;

class ClosureMapper[K1,V1,K2,V2] extends MapRunnable[Writable,Writable,Writable,Writable] {
  import Hadoop._;
  var m :Mapper[K1,V1,K2,V2] = _;

  override def configure(conf : JobConf) {
    val mapString = conf.get("smr.job.mapper.file");
    val localFiles = DistributedCache.getLocalCacheFiles(conf);
    println(localFiles.mkString(","));
    val mapFile = localFiles.filter(_.toString==mapString)(0);
    val inputStream = new java.io.ObjectInputStream(mapFile.getFileSystem(conf).open(mapFile));
    m = inputStream.readObject().asInstanceOf[Mapper[K1,V1,K2,V2]];
    inputStream.close();
  }
  final override def run(
    input: RecordReader[Writable,Writable],
    output : OutputCollector[Writable,Writable],
    r : Reporter) {
    var i = 0;
    val it = Util.iteratorFromProducer { () =>
      i+=1
      if(i%100==0) r.progress();
      val k = input.createKey().asInstanceOf[Writable];
      val v = input.createValue().asInstanceOf[Writable];
      input.next(k,v) match {
        case true => Some((Magic.wireToReal(k).asInstanceOf[K1],Magic.wireToReal(v).asInstanceOf[V1]))
        case false => None
      }
    }
    m.map(it).foreach { case (k2,v2) =>
      output.collect(Magic.realToWire(k2),Magic.realToWire(v2));
    }
  }
}
