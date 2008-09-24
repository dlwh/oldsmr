package smr.hadoop;

import org.apache.hadoop.io._;
import java.io._;

class AnyWritable[T](var value : T) extends WritableComparable[AnyWritable[T]] { 
  
  def get() = value;

  def this() = this(null.asInstanceOf[T]);

  @throws(classOf[java.io.IOException])
  override def write(out : DataOutput) {
    val bytesOut = new ByteArrayOutputStream();
    val bOut =  new ObjectOutputStream(bytesOut);
    bOut.writeObject(value);
    bOut.close();
    val arr = bytesOut.toByteArray;
    out.writeInt(arr.size);
    out.write(arr);
  }

  @throws(classOf[java.io.IOException])
  override def readFields(in : DataInput) {
    val size = in.readInt();
    val barr = new Array[Byte](size);
    in.readFully(barr);
    val bIn = new ObjectInputStream(new ByteArrayInputStream(barr));
    value = bIn.readObject.asInstanceOf[T]
  }

   def compareTo(o : AnyWritable[T]) :Int = {
    value.asInstanceOf[{ def compareTo(o : Any): Int}].compareTo(o.value);
  }
}
