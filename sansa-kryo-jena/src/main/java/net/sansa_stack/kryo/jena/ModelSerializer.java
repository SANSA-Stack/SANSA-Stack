package net.sansa_stack.kryo.jena;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Riot-based serializer for Models.
 *
 * @author Claus Stadler
 */
public class ModelSerializer
        extends RiotSerializerBase<Model> {

    public ModelSerializer(Lang lang, RDFFormat format) {
        super(lang, format);
    }

    @Override
    protected void writeActual(Model model, OutputStream out) {
        RDFDataMgr.write(out, model, format);
    }

    @Override
    protected Model readActual(InputStream in) {
        Model result = ModelFactory.createDefaultModel();
        RDFDataMgr.read(result, in, lang);
        return result;
    }
}
