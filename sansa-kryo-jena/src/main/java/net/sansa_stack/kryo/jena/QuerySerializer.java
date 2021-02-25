package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;

/**
 * Serializer for Query objects. Uses ARQ syntax as it is the most lenient one.
 *
 * @author Claus Stadler
 */
public class QuerySerializer extends Serializer<Query> {
    @Override
    public void write(Kryo kryo, Output output, Query obj) {
        output.writeString(obj.toString());
    }

    @Override
    public Query read(Kryo kryo, Input input, Class<Query> objClass) {
        String queryStr = input.readString(); // kryo.readClass(input).asInstanceOf[String]

        // We use syntaxARQ as for all practical purposes it is a superset of
        // standard SPARQL
        Query result = QueryFactory.create(queryStr, Syntax.syntaxARQ);
        return result;
    }
}
