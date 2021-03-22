package net.sansa_stack.kryo.jena;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Riot-based serializer for PrefixMapping (presently always deserializes as PrefixMappingImpl).
 *
 * @author Claus Stadler
 */
public class PrefixMappingSerializer
        extends RiotSerializerBase<PrefixMapping> {

    public PrefixMappingSerializer(Lang lang, RDFFormat format) {
        super(lang, format);
    }

    @Override
    protected void writeActual(PrefixMapping prefixMapping, OutputStream out) {
        Model tmp = ModelFactory.createDefaultModel();
        tmp.setNsPrefixes(prefixMapping);
        RDFDataMgr.write(out, tmp, format);
    }

    @Override
    protected PrefixMapping readActual(InputStream in) {
        Model tmp = ModelFactory.createDefaultModel();
        RDFDataMgr.read(tmp, in, lang);

        PrefixMapping result = new PrefixMappingImpl();
        result.setNsPrefixes(tmp);
        return result;
    }
}
