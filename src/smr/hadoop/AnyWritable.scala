package smr.hadoop;

import org.apache.hadoop.io._;
import java.io._;

class AnyWritable[T](var value : T) extends ScrewWritableComparable { 
  
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

  protected def compareX(o : Object) :Int = o.asInstanceOf[Any] match {
    case other: AnyWritable[_] => get().asInstanceOf[{ def compareTo(o : Any): Int}].compareTo(other.value);
    case _ => throw new IllegalArgumentException(get() + " is not comparable");
  }
}
