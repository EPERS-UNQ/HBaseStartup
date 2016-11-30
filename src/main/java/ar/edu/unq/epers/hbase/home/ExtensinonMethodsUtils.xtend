package ar.edu.unq.epers.hbase.home

import java.math.BigDecimal
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.uqbar.commons.utils.ReflectionUtils

class ExtensinonMethodsUtils {
	
	def static byte[] toBytes(Object object){
		switch object{
	      String: Bytes.toBytes(object)
	      Integer: Bytes.toBytes(object)
	      Short: Bytes.toBytes(object)
	      Long: Bytes.toBytes(object)
	      Double: Bytes.toBytes(object)
	      Float: Bytes.toBytes(object)
	      BigDecimal: Bytes.toBytes(object)
	    }
	}
	
	def static getId(Object object){
		get(object, "id")
	}
	
	def static setId(Object object, Object value){
		set(object, "id",value)
	}

	def static get(Object object, String property){
		ReflectionUtils.invokeGetter(object, property)
	}

	def static set(Object object, String property, Object value){
		ReflectionUtils.invokeSetter(object, property ,value)
	}	
	
	def static tableName(Class<?> type){
		TableName.valueOf(type.simpleName)
	}
	
	def static <T> emptyConstructor(Class<T> type){
		type.constructors.findFirst[it.parameterTypes.size == 0]
	}
}