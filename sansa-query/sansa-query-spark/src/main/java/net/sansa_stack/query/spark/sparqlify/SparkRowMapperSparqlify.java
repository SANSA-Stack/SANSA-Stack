package net.sansa_stack.query.spark.sparqlify;

import com.google.common.collect.Multimap;
import org.aksw.jena_sparql_api.views.RestrictedExpr;
import org.aksw.sparqlify.core.sparql.ItemProcessorSparqlify;
import org.aksw.sparqlify.core.sparql.RowMapperSparqlifyBinding;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class SparkRowMapperSparqlify
	implements Function<Row, Binding>, Serializable
{
	private static final long serialVersionUID = 1L;

	protected Multimap<Var, RestrictedExpr> varDef;

	public SparkRowMapperSparqlify(Multimap<Var, RestrictedExpr> varDef) {
		super();
		this.varDef = varDef;
	}

	@Override
	public Binding call(Row row) {
		Binding rawBinding = rowToRawBinding(row);
		Binding result = ItemProcessorSparqlify.process(varDef, rawBinding);

//		System.out.println("VarDef: " + varDef);
//		System.out.println("Row: " + row);
//		System.out.println("RawBinding: " + rawBinding);
//		System.out.println("ResultBinding: " + result);

		return result;
	}

	public static Binding rowToRawBinding(Row row) {
		BindingBuilder result = BindingFactory.builder();

		String[] fieldNames = row.schema().fieldNames();
		for (int i = 0; i < fieldNames.length; ++i) {
			Object v = row.get(i);
			String fieldName = fieldNames[i];
			int j = i + 1;
			RowMapperSparqlifyBinding.addAttr(result, j, fieldName, v);
		}

		return result.build();
	}
}
