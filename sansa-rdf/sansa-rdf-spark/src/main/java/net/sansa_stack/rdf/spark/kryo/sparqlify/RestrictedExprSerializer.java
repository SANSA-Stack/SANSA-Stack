package net.sansa_stack.rdf.spark.kryo.sparqlify;

import org.aksw.jena_sparql_api.views.RestrictedExpr;

import org.apache.jena.sparql.expr.Expr;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RestrictedExprSerializer
	extends Serializer<RestrictedExpr> {

	@Override
	public RestrictedExpr read(Kryo kryo, Input input, Class<RestrictedExpr> clazz) {
        Expr expr = (Expr)kryo.readClassAndObject(input);
        RestrictedExpr result = new RestrictedExpr(expr);
        return result;
	}

	@Override
	public void write(Kryo kryo, Output output, RestrictedExpr obj) {
        kryo.writeClassAndObject(output, obj.getExpr());
	}
}
