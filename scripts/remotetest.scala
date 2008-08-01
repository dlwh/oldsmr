import scala.actors.Actor._;
import scala.actors.remote.RemoteActor._;
import scala.actors.remote.Node;
val x = actor {
  scala.actors.Debug.level = 10; 
  try {
    alive(9010)
    register('myName,self);
  } catch {
    case x => println(x);
  }
  react { 
    case _ => reply { 1}
    react {
      case x => println(x); reply {'done}
    }}
}
classLoader = x.getClass.getClassLoader

try {
x !? None // make sure we're ready
val c = select(Node("128.12.89.161",9010),'myName)
c ! 'test // this line doesn't seem to return.
} catch {
  case e => println(e);
}
