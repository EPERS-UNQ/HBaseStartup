package ar.edu.unq.epers.hbase.model

import org.eclipse.xtend.lib.annotations.Accessors
import java.util.List
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

@Accessors
class CachePorEmail {
	String id
	String email
	List<Persona> personas
	
	
	def getPersonasSerializadas(){
		new Gson().toJson(personas)
	}
	
	def void setPersonasSerializadas(String json){
		personas = new Gson().fromJson(json, new TypeToken<List<Persona>>(){}.getType())
	}
}

