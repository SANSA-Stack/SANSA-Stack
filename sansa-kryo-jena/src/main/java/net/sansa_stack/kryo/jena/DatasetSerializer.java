package net.sansa_stack.kryo.jena;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Riot-based serializer for Datasets.
 *
 * @author Claus Stadler
 */
public class DatasetSerializer
        extends RiotSerializerBase<Dataset> {

    public DatasetSerializer(Lang lang, RDFFormat format) {
        super(lang, format);
    }

    @Override
    protected void writeActual(Dataset obj, OutputStream out) {
        RDFDataMgr.write(out, obj, format);
    }

    @Override
    protected Dataset readActual(InputStream in) {
        Dataset result = DatasetFactory.create();
        RDFDataMgr.read(result, in, lang);
        return result;
    }
}
