package net.sansa_stack.query.spark.sparqlify;

import com.esotericsoftware.kryo.Kryo;
import net.sansa_stack.rdf.spark.kryo.sparqlify.RestrictedExprSerializer;
import org.apache.spark.serializer.KryoRegistrator;

public class KryoRegistratorSparqlify
	implements KryoRegistrator
{

	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(org.aksw.jena_sparql_api.views.RestrictedExpr.class, new RestrictedExprSerializer());
	}

}
