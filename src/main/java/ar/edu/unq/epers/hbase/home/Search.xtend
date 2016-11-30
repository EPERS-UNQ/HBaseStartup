package ar.edu.unq.epers.hbase.home

import java.util.ArrayList
import java.util.List
import java.util.Map
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.eclipse.xtend.lib.annotations.Accessors

import static extension ar.edu.unq.epers.hbase.home.ExtensinonMethodsUtils.*

@Accessors
class Search<T>{
	List<Filter> filters = new ArrayList()
	HTable table
	ResultScanner result
	Class<T> type
	FilterList.Operator operator

	new (Class<T> type, HTable table){
		this.table = table
		this.type = type
		and
	}
	
	def addFilter(String family, String property, CompareOp operation, ByteArrayComparable value){
		filters.add(new SingleColumnValueFilter(
			family.toBytes, 
			property.toBytes, 
			operation, 
			value))
		this
	}

	def addFilter(String family, String property, CompareOp operation, Object value){
		addFilter(family, property, operation, new BinaryComparator(value.toBytes))
	}
	
	def and(){
		operator = FilterList.Operator.MUST_PASS_ALL
		this
	}
	def or(){
		operator = FilterList.Operator.MUST_PASS_ONE
		this
	}
	
	def getScan(){
		val scan = new Scan()
		scan.setFilter(new FilterList(operator, filters))
	}
	
	def T get(Map<String, List<String>> mapping){
		list(mapping).head	
	}
	
	def List<T> list(Map<String, List<String>> mapping){
		result = table.getScanner(scan)
		val list = result.map[res|
			val t = type.emptyConstructor.newInstance() as T
			t.id = new String(res.row)
			mapping.keySet.forEach[ family |
				mapping.get(family).forEach[ property |
					t.set(property, new String(res.getValue(family.toBytes, property.toBytes)))
				]
			]
			t
		].toList
		result.close
		list
	}
	
	
}