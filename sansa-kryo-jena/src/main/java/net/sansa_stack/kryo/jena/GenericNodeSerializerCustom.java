package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.vocabulary.XSD;

/**
 * A node serializer using a custom (i.e. non-standard) format for intermediate serialization.
 * This class was created in order to allow processing of invalid RDF data.
 * Examples include IRIs with white spaces or even worse IRIs with angular brackets which would
 * result in a non-parsable turtle serialization.
 *
 * @author Claus Stadler
 */
public class GenericNodeSerializerCustom
    extends Serializer<Node>
{
    public static final int TYPE_MASK    = 0xf0;
    public static final int TYPE_IRI     = 0x10;
    public static final int TYPE_BNODE   = 0x20;
    public static final int TYPE_LITERAL = 0x30;
    public static final int TYPE_VAR     = 0x40;

    public static final int SUBTYPE_MASK = 0x0f;
    public static final int LITERAL_HAS_LANG = 0x01;
    public static final int LITERAL_HAS_DTYPE = 0x02;

    protected TypeMapper typeMapper;

    public GenericNodeSerializerCustom() {
        this(TypeMapper.getInstance());
    }

    public GenericNodeSerializerCustom(TypeMapper typeMapper) {
        this.typeMapper = typeMapper;
    }

    @Override
    public void write(Kryo kryo, Output output, Node node) {
        if (node.isURI()) {
            output.writeByte(TYPE_IRI);
            output.writeString(node.getURI());
        } else if (node.isLiteral()) {
            String lex = node.getLiteralLexicalForm();
            String lang = node.getLiteralLanguage();
            String dt = node.getLiteralDatatypeURI();

            if (lang != null && !lang.isEmpty()) {
                output.writeByte(TYPE_LITERAL | LITERAL_HAS_LANG);
                output.writeString(lex);
                output.writeString(lang);
            } else if (dt != null && !dt.isEmpty() && !dt.equals(XSD.xstring.getURI())) {
                output.writeByte(TYPE_LITERAL | LITERAL_HAS_DTYPE);
                output.writeString(lex);
                output.writeString(dt);
            } else {
                output.writeByte(TYPE_LITERAL);
                output.writeString(lex);
            }
        } else if (node.isBlank()) {
            output.writeByte(TYPE_BNODE);
            output.writeString(node.getBlankNodeLabel());
        } else if (node.isVariable()) {
            output.writeByte(TYPE_VAR);
            output.writeString(node.getName());
        } else {
            throw new RuntimeException("Unknown node type: " + node);
        }
    }

    @Override
    public Node read(Kryo kryo, Input input, Class<Node> cls) {
        Node result;
        String v1, v2;

        byte type = input.readByte();

        int typeVal = type & TYPE_MASK;
        switch (typeVal) {
            case TYPE_IRI:
                v1 = input.readString();
                result = NodeFactory.createURI(v1);
                break;
            case TYPE_LITERAL:
                int subTypeVal = type & SUBTYPE_MASK;
                switch (subTypeVal) {
                    case 0:
                        v1 = input.readString();
                        result = NodeFactory.createLiteral(v1);
                        break;
                    case LITERAL_HAS_LANG:
                        v1 = input.readString();
                        v2 = input.readString();
                        result = NodeFactory.createLiteral(v1, v2);
                        break;
                    case LITERAL_HAS_DTYPE:
                        v1 = input.readString();
                        v2 = input.readString();
                        RDFDatatype dtype = typeMapper.getSafeTypeByName(v2);
                        result = NodeFactory.createLiteral(v1, dtype);
                        break;
                    default:
                        throw new RuntimeException("Unknown literal sub-type: " + subTypeVal);
                }
                break;
            case TYPE_BNODE:
                v1 = input.readString();
                result = NodeFactory.createBlankNode(v1);
                break;
            case TYPE_VAR:
                v1 = input.readString();
                result = Var.alloc(v1); // NodeFactory.createVariable ?
                break;
            default:
                throw new RuntimeException("Unknown node type: " + typeVal);
        }
        return result;
    }
}
