package smr.plugin;
import scala.tools.nsc
import nsc.Global
import nsc.Phase
import nsc.plugins.Plugin
import nsc.plugins.PluginComponent

class SerOverride(val global: Global) extends Plugin {
  import global._

  val name = "seroverride"
  val description = "Makes all closures serializable"
  val components = List[PluginComponent](Component)
  
  private object Component extends PluginComponent {
    val global = SerOverride.this.global
    val runsAfter = "mixin"
    val phaseName = SerOverride.this.name
    def newPhase(prev: Phase) = new SerOverridePhase(prev)    
  }
  
  private class SerOverridePhase(prev: Phase) extends Phase(prev) {
    def name = SerOverride.this.name
    def run {
      for (unit <- currentRun.units; 
           cdef @ ClassDef(_,name,tparams,impl) <- unit.body) {
        val sym = cdef.symbol 
        if(sym.name.toString.contains("anonfun$")) {
          sym.attributes= AnnotationInfo(definitions.SerializableAttr.tpe, List(), List()) :: sym.attributes 
        }
      }
    }
  }
}

