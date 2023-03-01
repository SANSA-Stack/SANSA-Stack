package net.sansa_stack.query.spark.sparqlify;

import com.esotericsoftware.kryo.Kryo;
import net.sansa_stack.rdf.spark.kryo.sparqlify.RestrictedExprSerializer;
import org.aksw.commons.util.list.ListUtils;
import org.aksw.jena_sparql_api.views.E_RdfTerm;
import org.aksw.jenax.io.kryo.jena.ExprFunctionNSerializer;
import org.apache.spark.serializer.KryoRegistrator;

public class KryoRegistratorSparqlify
	implements KryoRegistrator
{

	@Override
	public void registerClasses(Kryo kryo) {
		// HashMultimapSerializer.registerSerializers(kryo);

		kryo.register(org.aksw.jena_sparql_api.views.RestrictedExpr.class, new RestrictedExprSerializer());
		kryo.register(org.aksw.jena_sparql_api.views.E_RdfTerm.class, new ExprFunctionNSerializer<>(args -> new E_RdfTerm(
				ListUtils.getOrNull(args, 0), ListUtils.getOrNull(args, 1), ListUtils.getOrNull(args, 2), ListUtils.getOrNull(args, 3))));
	}

}
