package smr;

/**
* More scala like ThreadLocal storage. Also, it's serializable, to be more smr friendly.
*
* @author(dlwh)
*/
@serializable
abstract class ThreadLocal[T] extends Function0[T] {
  @transient // var because of serialization constraints
  private var tl = new java.lang.ThreadLocal[T] {
    override def initialValue = default();
  }

  @throws(classOf[java.io.IOException])
  @throws(classOf[ClassNotFoundException])
  private def readObject(in : java.io.ObjectInputStream) {
    tl = new java.lang.ThreadLocal[T] {
      override def initialValue = default();
    }
  }

  // must be overridden
  protected def default(): T;

  def apply() = tl.get();
  def get() = tl.get();

  def value = tl.get();
  def value_=(v : T) = tl.set(v);
}
