package smr.plugin;
import scala.tools.nsc
import nsc.Global
import nsc.Phase
import nsc.plugins.Plugin
import nsc.plugins.PluginComponent
import nsc.transform._
import nsc.symtab.Flags._

class SerOverride(val global: Global) extends Plugin {
  import global._

  val name = "seroverride"
  val description = "Makes all closures serializable"
  val components = List[PluginComponent](Component);
  
  private object Component extends PluginComponent {
    val global = SerOverride.this.global
    val runsAfter = "explicitouter"
    val phaseName = SerOverride.this.name
    def newPhase(prev: Phase) = new SerOverridePhase(prev)    
  }

  private class SerTransformer extends Transformer { 
    override def transform(t : Tree):Tree = t match {
      case cdef@ ClassDef(mods,name,tparams,impl) =>
        val sym = cdef.symbol 
        val serType = definitions.SerializableAttr.tpe
        if( sym.hasFlag(SYNTHETIC) && sym.name.toString.contains("anonfun") && !sym.attributes.exists(serType==_.atp)) {
          sym.attributes= AnnotationInfo(serType, List(), List()) :: sym.attributes 
          copy.ClassDef(t, mods, name, transformTypeDefs(tparams), transformTemplate(impl)) ;
        } else {
          super.transform(t);
        }
      case _ => super.transform(t);
    }
  }

  private class SerOverridePhase(prev: Phase) extends Phase(prev) {
    def name = SerOverride.this.name
    def run {
      val trans = new SerTransformer;
      for(unit <- currentRun.units)  {
        unit.body = trans.transform(unit.body)
      }
    }
  }
}
