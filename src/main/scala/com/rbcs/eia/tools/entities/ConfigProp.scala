package com.rbcs.eia.tools.entities
import scala.beans.BeanProperty
class ConfigProp {

		    @BeanProperty var dataPath = ""
		    @BeanProperty var doIndex = ""
		    @BeanProperty var indexName = ""
        @BeanProperty var mappingName = ""
        @BeanProperty var esNodes = ""
        @BeanProperty var esClustername = ""
        @BeanProperty var esMappingId = ""
        @BeanProperty var esIndexAutoCreate =""
        @BeanProperty var columnsToConvert = new java.util.ArrayList[String]()
		    override def toString: String = s"$dataPath: $dataPath, mappingName: $mappingName"
		    
		    
		    
		    
}