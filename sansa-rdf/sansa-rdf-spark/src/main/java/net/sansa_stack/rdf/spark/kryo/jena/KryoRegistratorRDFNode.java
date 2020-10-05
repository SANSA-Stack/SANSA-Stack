package net.sansa_stack.rdf.spark.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.google.gson.Gson;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.LiteralImpl;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.spark.serializer.KryoRegistrator;

// TODO Merge into JenaKryoRegistrator in sansa-rdf
public class KryoRegistratorRDFNode
	implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo kryo) {
		registerClassesActual(kryo);
	}

	public static void registerClassesActual(Kryo kryo) {
		Gson gson = new Gson();

		//kryo.register(org.apache.jena.rdf.model.RDFNode.class, new RDFNodeSerializer<>(Function.identity(), gson));
		//kryo.register(org.apache.jena.rdf.model.Resource.class, new RDFNodeSerializer<>(RDFNode::asResource, gson));
		//kryo.register(org.apache.jena.rdf.model.impl.R.class, new RDFNodeSerializer<>(RDFNode::asResource, gson));
		kryo.register(ResourceImpl.class, new RDFNodeSerializer<>(RDFNode::asResource, gson));
		kryo.register(PropertyImpl.class, new RDFNodeSerializer<>(n -> ResourceFactory.createProperty(n.asResource().getURI()), gson));
		kryo.register(LiteralImpl.class, new RDFNodeSerializer<>(RDFNode::asLiteral, gson));

		// kryo.register(Property.class, new RDFNodeSerializer<>(n -> ResourceFactory.createProperty(n.asResource().getURI()), gson));
	}
}