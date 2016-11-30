package ar.edu.unq.epers.hbase.model

import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.EqualsHashCode

@Accessors
@EqualsHashCode
class Persona {
	String nombre
	String apellido
	String email
	String id
	
	new (){
	}
	
	new (String id, String nombre, String apellido, String email){
		this.id = id
		this.nombre = nombre
		this.apellido = apellido
		this.email = email
	}
	
	
}