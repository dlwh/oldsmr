package smr;

/**
* Class to make doing compareTo a little less painful;
*
*/
@serializable
abstract class Order[T](elems : (T => Comparable[_])*) extends Ordered[T] with Comparable[T] { this : T=>
  override def compare(o : T) = {
    Order.recursiveCompare( (0 until elems.length) map ( i =>  (elems(i)(this),elems(i)(o))));
  }
}

object Order {
  private def recursiveCompare(o : Seq[(Any,Any)]):Int = {
    if(o.length == 0) 0
    else o(0)._1.asInstanceOf[Comparable[Any]].compareTo(o(0)._2) match {
      case 0 => recursiveCompare(o.drop(1));
      case x => x;
    }
  }
}
