package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.google.gson.Gson;
import org.apache.jena.graph.*;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.LiteralImpl;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.jena.sparql.core.DatasetImpl;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.nodevalue.*;

/**
 * Note: KryoRegistrator is an interface introduced by spark; hence we cannot use it
 * in this common package.
 */
public class JenaKryoRegistratorLib {
    public static void registerClasses(Kryo kryo) {
        // The NodeValue list should be complete for jena 3.17.0
        Serializer<Expr> exprSerializer = new ExprSerializer();

        kryo.register(NodeValueBoolean.class, exprSerializer);
        kryo.register(NodeValueDecimal.class, exprSerializer);
        kryo.register(NodeValueDouble.class, exprSerializer);
        kryo.register(NodeValueDT.class, exprSerializer);
        kryo.register(NodeValueDuration.class, exprSerializer);
        kryo.register(NodeValueFloat.class, exprSerializer);
        kryo.register(NodeValueInteger.class, exprSerializer);
        kryo.register(NodeValueLang.class, exprSerializer);
        kryo.register(NodeValueNode.class, exprSerializer);
        kryo.register(NodeValueSortKey.class, exprSerializer);
        kryo.register(NodeValueString.class, exprSerializer);

        // This list is incomplete - use class path scanning?
        // However this would probably slows down startup time alot for that number of classes...
        kryo.register(ExprVar.class, exprSerializer);
        kryo.register(E_Equals.class, exprSerializer);
        kryo.register(E_BNode.class, exprSerializer);
        kryo.register(E_Datatype.class, exprSerializer);
        kryo.register(E_IRI.class, exprSerializer);
        kryo.register(E_Datatype.class, exprSerializer);
        kryo.register(E_StrConcat.class, exprSerializer);
        kryo.register(Expr.class, exprSerializer);

        kryo.register(VarExprList.class, new VarExprListSerializer());

        kryo.register(Query.class, new QuerySerializer());

        // Using allowValues false in order to retain RDF terms exactly
        // Serializer<Node> nodeSerializer = new GenericNodeSerializerViaThrift(false);
        Serializer<Node> nodeSerializer = new GenericNodeSerializerCustom();

        registerNodeSerializers(kryo, nodeSerializer);
        kryo.register(Var.class, new VarSerializer());
        kryo.register(Node_Variable.class, new VariableNodeSerializer());
        kryo.register(Node_ANY.class, new ANYNodeSerializer());

        kryo.register(Node[].class); //, new NodeArraySerializer());

        kryo.register(Triple.class, new TripleSerializer());
        kryo.register(org.apache.jena.graph.Triple[].class);

        kryo.register(PrefixMappingImpl.class, new PrefixMappingSerializer(Lang.TURTLE, RDFFormat.TURTLE_PRETTY));

        kryo.register(ModelCom.class, new ModelSerializer(Lang.RDFTHRIFT, RDFFormat.RDF_THRIFT_VALUES));
        kryo.register(DatasetImpl.class, new DatasetSerializer(Lang.RDFTHRIFT, RDFFormat.RDF_THRIFT_VALUES));

        Gson gson = new Gson();

        //kryo.register(org.apache.jena.rdf.model.RDFNode.class, new RDFNodeSerializer<>(Function.identity(), gson));
        //kryo.register(org.apache.jena.rdf.model.Resource.class, new RDFNodeSerializer<>(RDFNode::asResource, gson));
        //kryo.register(org.apache.jena.rdf.model.impl.R.class, new RDFNodeSerializer<>(RDFNode::asResource, gson));
        kryo.register(ResourceImpl.class, new RDFNodeSerializer<>(RDFNode::asResource, gson));
        kryo.register(PropertyImpl.class, new RDFNodeSerializer<>(n -> ResourceFactory.createProperty(n.asResource().getURI()), gson));
        kryo.register(LiteralImpl.class, new RDFNodeSerializer<>(RDFNode::asLiteral, gson));
    }


    public static void registerNodeSerializers(Kryo kryo, Serializer<Node> nodeSerializer) {
        kryo.register(Node.class, nodeSerializer);
        kryo.register(Node_Blank.class, nodeSerializer);
        kryo.register(Node_URI.class, nodeSerializer);
        kryo.register(Node_Literal.class, nodeSerializer);
    }
}
