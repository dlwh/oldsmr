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


private class ReduceWrapper[K1,V1,K2,V2] extends Reducer[Writable,Writable,Writable,Writable] {
  import Hadoop._;
  var r : Reduce[K1,V1,K2,V2] = _;
  override def configure(conf : JobConf) { 
    val mapString = conf.get("smr.job.reducer.file");
    val localFiles = DistributedCache.getCacheFiles(conf);
    println(localFiles.mkString(","));
    val mapFile = new Path(localFiles.filter(_.toString==mapString)(0).toString);
    val inputStream = new java.io.ObjectInputStream(mapFile.getFileSystem(conf).open(mapFile));
    r = inputStream.readObject().asInstanceOf[Reduce[K1,V1,K2,V2]];
    inputStream.close();
  }

  override def reduce(k : Writable,
    it: java.util.Iterator[Writable],
    output : OutputCollector[Writable,Writable],
    rp: Reporter) {
    val newK = Magic.wireToReal(k).asInstanceOf[K1];

    val it2 = new Iterator[V1] {
      def hasNext = it.hasNext;
      def next = { 
        i+=1;
        if(i%100 ==0) rp.progress();
        Magic.wireToReal(it.next).asInstanceOf[V1];
      }
      var i = 0;
    }
    r.reduce(newK,it2).foreach { case (k,v)=>
      output.collect(Magic.realToWire(k),Magic.realToWire(v));
    }
  }
  def close() {}
}
