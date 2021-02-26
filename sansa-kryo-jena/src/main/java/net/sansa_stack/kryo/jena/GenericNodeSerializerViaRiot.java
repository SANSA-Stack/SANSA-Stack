package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.atlas.io.IndentedLineBuffer;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.RIOT;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.*;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerText;
import org.apache.jena.sparql.ARQConstants;

/**
 * Kryo Serializer for Node using riot.
 *
 * @author Lorenz Buehmann
 */
public class GenericNodeSerializerViaRiot extends Serializer<Node> {

    /** A {@link PrefixMap} (rather than {@link org.apache.jena.shared.PrefixMapping}) of standard prefixes */
    private static final PrefixMap pmap = PrefixMapFactory.createForInput();

    static {
        pmap.add("rdf", ARQConstants.rdfPrefix);
        pmap.add("rdfs", ARQConstants.rdfsPrefix);
        pmap.add("xsd", ARQConstants.xsdPrefix);
        pmap.add("owl", ARQConstants.owlPrefix);
    }

    protected ErrorHandler errorHandler = ErrorHandlerFactory.errorHandlerWarn;
    protected ParserProfile profile = setupInternalParserProfile();

    //    val nodeFormatter = new NodeFormatterTTL(null, pmap, NodeToLabel.createBNodeByLabelEncoded())
    NodeFormatter nodeFormatter = new NodeFormatterNT();
    IndentedLineBuffer writer = new IndentedLineBuffer();

    @Override
    public void write(Kryo kryo, Output output, Node obj) {

        //      println(s"serializing node $obj   => ${FmtUtils.stringForNode(obj)}")
        nodeFormatter.format(writer, obj);
        output.writeString(writer.toString());
        writer.clear();
    }

    @Override
    public Node read(Kryo kryo, Input input, Class<Node> objClass) {
        String s = input.readString();
        Node node = parse(s);
        //      println(s"deserializing string $s   => $n")
        return node;
    }

    public Node parse(String string) {
        Tokenizer tokenizer = TokenizerText.create()
                .errorHandler(errorHandler)
                .fromString(string).build();

        Node n;
        if (!tokenizer.hasNext()) {
            n = null;
        } else {
            Token t = tokenizer.next();
            n = profile.create(null, t);

        }
        return n;
    }

    protected ParserProfile setupInternalParserProfile() {
        LabelToNode labelToNode = LabelToNode.createUseLabelEncoded();
        FactoryRDF factoryRDF = RiotLib.factoryRDF(labelToNode);
        ParserProfile result = new ParserProfileStd(factoryRDF, errorHandler, IRIResolver.create(), pmap, RIOT.getContext().copy(), true, false);
        return result;
    }
}
