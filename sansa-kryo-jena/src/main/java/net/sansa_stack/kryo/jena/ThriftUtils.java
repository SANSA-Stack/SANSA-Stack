package net.sansa_stack.kryo.jena;

import org.apache.jena.graph.Node;
import org.apache.jena.riot.thrift.TRDF;
import org.apache.jena.riot.thrift.ThriftConvert;
import org.apache.jena.riot.thrift.wire.RDF_Term;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Utils to read/write RDF objects. Used in kryo serialization.
 *
 * @author Claus Stadler
 */
public class ThriftUtils {
    public static byte[] writeNode(Node node, boolean allowValues) {
        RDF_Term rdfTerm = ThriftConvert.convert(node, allowValues);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TProtocol protocol = TRDF.protocol(out);
        try {
            rdfTerm.write(protocol);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        TRDF.flush(protocol);
        byte[] result = out.toByteArray();
        return result;
    }

    public static Node readNode(byte[] bytes) {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        TProtocol protocol = TRDF.protocol(in);
        RDF_Term rdfTerm = new RDF_Term();
        try {
            rdfTerm.read(protocol);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        Node result = ThriftConvert.convert(rdfTerm);
        return result;
    }
}
