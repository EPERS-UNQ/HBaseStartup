package ar.edu.unq.epers

import ar.edu.unq.epers.hbase.home.HBaseHome
import ar.edu.unq.epers.hbase.home.Search
import ar.edu.unq.epers.hbase.model.CachePorEmail
import ar.edu.unq.epers.hbase.model.Persona
import java.util.List
import java.util.Map
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class HBaseTest {
	HBaseHome home
	Persona persona1
	Persona persona2
	Persona persona3
	Persona persona4
	Persona persona5
	Persona persona6
	Map<String, List<String>> personaMapping
	Map<String, List<String>> cacheMapping

	@Before
	def void setup() {
		home = new HBaseHome()
		crearTablaPersona
		crearTablaCache
	}

	@Test
	def void filtrarPorNombre() {
		val table = home.getTable(Persona)
		val search = new Search(Persona, table)
		search.addFilter("nombre", "nombre", CompareOp.EQUAL, "Rae")

		val results = search.list(personaMapping)

		Assert.assertEquals(results.length, 1)
		Assert.assertTrue(results.contains(persona4))

		table.close()
	}

	@Test
	def void filtrarPorEmail() {
		val table = home.getTable(Persona)
		val emailFilter = new RegexStringComparator("contoso.com");
		val search = new Search(Persona, table)
		search.addFilter("datosContacto", "email", CompareOp.EQUAL, emailFilter)

		val results = search.list(personaMapping)

		Assert.assertEquals(results.length, 3)
		Assert.assertTrue(results.contains(persona2))
		Assert.assertTrue(results.contains(persona4))
		Assert.assertTrue(results.contains(persona6))

		table.close()
	}
	
	@Test
	def void filtrarPorNombreYApellido() {
		val table = home.getTable(Persona)
		val search = new Search(Persona, table)
		search.addFilter("nombre", "nombre", CompareOp.EQUAL, "Rosalie")
		search.addFilter("nombre", "apellido", CompareOp.EQUAL, "burton")

		val persona = search.get(personaMapping)

		Assert.assertEquals(persona.id, "5")
		Assert.assertEquals(persona.nombre, "Rosalie")
		Assert.assertEquals(persona.apellido, "burton")
		Assert.assertEquals(persona.email, "rosalie@fabrikam.com")
		
		table.close()
	}
	
		@Test
	def void filtrarPorNombreOApellido() {
		val table = home.getTable(Persona)
		val search = new Search(Persona, table).or
		search.addFilter("nombre", "nombre", CompareOp.EQUAL, "Rae")
		search.addFilter("nombre", "apellido", CompareOp.EQUAL, "Haddad")

		val personas = search.list(personaMapping)

		Assert.assertEquals(personas.length, 2)
		Assert.assertTrue(personas.contains(persona1))
		Assert.assertTrue(personas.contains(persona4))
		
		table.close()
	}

	@Test
	def void cachePorEmail() {
		val table = home.getTable(CachePorEmail)
		val search = new Search(CachePorEmail, table)
		search.addFilter("reporte", "email", CompareOp.EQUAL, "contoso.com")

		val cache = search.get(cacheMapping)

		Assert.assertEquals(cache.id, "1")
		Assert.assertEquals(cache.email, "contoso.com")
		Assert.assertEquals(cache.id, "1")
		Assert.assertEquals(cache.personas.length, 3)
		Assert.assertTrue(cache.personas.contains(persona2))
		Assert.assertTrue(cache.personas.contains(persona4))
		Assert.assertTrue(cache.personas.contains(persona6))

		table.close()
	}

	@After
	def void deleteData() {
		home.deleteTable(Persona)
		home.deleteTable(CachePorEmail)
	}

	def crearTablaPersona() {
		home.createTable(Persona, "nombre", "datosContacto")
		persona1 = new Persona("1", "Marcel", "Haddad", "marcel@fabrikam.com")
		persona2 = new Persona("2", "Franklin", "Holtz", "franklin@contoso.com")
		persona3 = new Persona("3", "Franklin", "McKee", "dwayne@fabrikam.com")
		persona4 = new Persona("4", "Rae", "Schroeder", "rae@contoso.com")
		persona5 = new Persona("5", "Rosalie", "burton", "rosalie@fabrikam.com")
		persona6 = new Persona("6", "Gabriela", "Ingram", "gabriela@contoso.com")

		personaMapping = newHashMap(
			"nombre" -> #["nombre", "apellido"],
			"datosContacto" -> #["email"]
		)

		val peoples = #[persona1, persona2, persona3, persona4, persona5, persona6]

		val table = home.getTable(Persona)
		home.add(table, peoples, personaMapping)
		table.close()
	}

	def crearTablaCache() {
		home.createTable(CachePorEmail, "reporte")
		val table = home.getTable(CachePorEmail)

		val cache = new CachePorEmail => [
			id = "1"
			email = "contoso.com"
			personas = #[persona2, persona4, persona6]
		]

		cacheMapping = newHashMap(
			"reporte" -> #["email", "personasSerializadas"]
		)

		home.add(table, cache, cacheMapping)
		table.close()
	}

}