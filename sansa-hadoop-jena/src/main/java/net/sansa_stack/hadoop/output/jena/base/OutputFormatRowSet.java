package net.sansa_stack.hadoop.output.jena.base;

import com.google.common.base.Preconditions;
import org.aksw.jenax.io.rowset.core.RowSetStreamWriter;
import org.aksw.jenax.io.rowset.core.RowSetStreamWriterFactory;
import org.aksw.jenax.io.rowset.core.RowSetStreamWriterRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class OutputFormatRowSet
        extends OutputFormatBase<Binding>
{
    protected Lang getDefaultResultSetLang() {
        // TSV is lossless
        // XXX Could we ever support RS_JSON?
        return ResultSetLang.RS_TSV;
    }

    @Override
    protected RecordWriter<Long, Binding> getRecordWriter(Configuration conf, OutputStream out, FragmentOutputSpec fragmentOutputSpec) {
        Lang defaultLang = getDefaultResultSetLang();
        Lang lang = RdfOutputUtils.getLang(conf, defaultLang);
        Preconditions.checkArgument(lang != null, "No output language configured");
        List<Var> vars = RdfOutputUtils.getVars(conf);
        Preconditions.checkArgument(vars != null, "Result set variables not set");
        RowSetStreamWriterFactory factory = RowSetStreamWriterRegistry.getFactory(lang);
        Preconditions.checkArgument(factory != null, "No result set writer registered for '" + lang + "'");
        RowSetStreamWriter writer = factory.create(out, vars);
        RecordWriterRowSetStream result;
        try {
            result = new RecordWriterRowSetStream(writer, fragmentOutputSpec);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
