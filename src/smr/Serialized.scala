package smr;
/**
* Tricks the compiler into thinking a class is serialized.
* The scala compiler team is convinced that only statically-verifiably
* serializable closures should be serialized. That often isn't good enough, so
* you should use this to trick the compiler into thinking that a val/var is 
* serializable so that you can serialize it.
*
* @author (dlwh)
*/
@serializable class Serialized[T](val value :T);

trait SerializedImplicits {
  implicit def serToValue[T](ser : Serialized[T]) = ser.value;
}

object Serialized extends SerializedImplicits {
  def apply[T](value :T) = new Serialized[T](value);
}
