package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.Gson;
import org.aksw.jena_sparql_api.io.json.RDFNodeJsonUtils;
import org.apache.jena.rdf.model.RDFNode;

import java.util.function.Function;


public class RDFNodeSerializer<T extends RDFNode>
        extends Serializer<T> {

    protected Gson gson;
    protected Function<? super RDFNode, T> fn;

    public RDFNodeSerializer(Function<? super RDFNode, T> fn, Gson gson) {
        super();
        this.fn = fn;
        this.gson = gson;
    }

    public T read(Kryo kryo, Input input, Class<T> clazz) {
        String jsonNodeLdString = input.readString();
        RDFNode rdfNode = RDFNodeJsonUtils.toRDFNode(jsonNodeLdString, gson);
        T result = fn.apply(rdfNode);
        return result;
    }

    public void write(Kryo kryo, Output output, T rdfNode) {
        String jsonNodeLdString = RDFNodeJsonUtils.toJsonNodeString(rdfNode, gson);
        output.writeString(jsonNodeLdString);
    }

}
