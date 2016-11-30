package ar.edu.unq.epers.hbase.home

import java.util.List
import java.util.Map
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.eclipse.xtend.lib.annotations.Accessors

import static extension ar.edu.unq.epers.hbase.home.ExtensinonMethodsUtils.*

@Accessors
class HBaseHome {
	val config = HBaseConfiguration.create()
	val admin = new HBaseAdmin(config)
	
	def getTable(String name){
		new HTable(config, name);
	}
	
	def  getTable(Class<?> type){
		getTable(type.simpleName)
	}
	
	def <T> void add(HTable table, List<T> ts, Map<String, List<String>> properties){
		ts.forEach[add(table, it, properties)]
	}
	
	def <T> void add(HTable table, T t, Map<String, List<String>> properties){
		val put = new Put(t.id.toBytes)
		properties.keySet.forEach[ family |
			properties.get(family).forEach[ property |
				put.addColumn(family.toBytes, property.toBytes, t.get(property).toBytes);
			]
			
		]
		table.put(put);
	}
	
	def createTable(Class<?> type, String... families){
		val table = new HTableDescriptor(type.tableName)
		families.forEach[table.addFamily(new HColumnDescriptor(it))]
		admin.createTable(table)
	}
	
	def addFamily(HTableDescriptor table, String name){
		table.addFamily(new HColumnDescriptor("name"));
	}
	
	def deleteTable(Class<?> type){
		deleteTable(type.simpleName)
	}
	
	def deleteTable(String name){
		admin.disableTable(name);
		admin.deleteTable(name);
	}
	
}
