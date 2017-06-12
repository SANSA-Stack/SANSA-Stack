package net.sansa_stack.query.flink.sparqlify

import com.google.common.collect.Multimap
import org.aksw.jena_sparql_api.views.RestrictedExpr
import org.aksw.sparqlify.core.sparql.{ItemProcessorSparqlify, RowMapperSparqlifyBinding}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.api.TableSchema
import org.apache.flink.types.Row
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingHashMap}

/**
  * Created by Simon Bin on 12/06/17.
  */
class FlinkRowMapperSparqlify(val varDef: Multimap[Var,RestrictedExpr], val schema: TableSchema) extends MapFunction[Row, Binding] with Serializable {
  override def map(row: Row): Binding = {
    val raw = new BindingHashMap()
    (0 until row.getArity).map( i => {
      RowMapperSparqlifyBinding.addAttr(raw, i + 1, schema.getColumnName(i).get, row.getField(i))
    }
    )
    val result = ItemProcessorSparqlify.process(varDef, raw)
    result
  }
}
