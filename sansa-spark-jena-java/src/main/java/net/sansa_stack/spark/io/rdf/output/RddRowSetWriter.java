package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.resultset.ResultSetWriter;
import org.apache.jena.riot.resultset.ResultSetWriterFactory;
import org.apache.jena.riot.resultset.ResultSetWriterRegistry;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetStream;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

public class RddRowSetWriter
    extends RddRowSetWriterSettings<RddRowSetWriter>
{
    private static final Logger logger = LoggerFactory.getLogger(RddRowSetWriter.class);

    // Cached attributes from the given RDD
    // protected JavaSparkContext sparkContext;
    protected JavaResultSetSpark rowSet;
    // protected Configuration hadoopConfiguration;

    public JavaResultSetSpark getRowSet() {
        return rowSet;
    }

    public RddRowSetWriter setRowSet(JavaResultSetSpark rowSet) {
        this.rowSet = rowSet;
        return self();
    }

    public void runActual(RddWriterSettings<?> effectiveSettings) {
        Lang finalLang = outputLang != null ? outputLang : Lang.TSV;

        RddRowSetWriterUtils.write(rowSet, effectiveSettings.getPartitionFolder(), finalLang);
    }

    /** Same as {@link #run()} but without the checked IOException */
    public void runUnchecked() {
        try {
            run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // @Override
    public void run() throws IOException {
        if (isConsoleOutput()) {
            runOutputToConsole();
        } else {
            runSpark();
        }
    }

    protected void runSpark() throws IOException {
        RddWriterSettings<?> effectiveSettings = RddWriterUtils.prepare(this, JavaSparkContextUtils.fromRdd(rowSet.getRdd()).hadoopConfiguration());
        runActual(effectiveSettings);
        RddWriterUtils.postProcess(effectiveSettings);
    }

    protected void runOutputToConsole() throws IOException {
        Lang finalLang = outputLang != null ? outputLang : Lang.TSV;

        ResultSetWriterFactory writerFactory = ResultSetWriterRegistry.getFactory(finalLang);
        ResultSetWriter writer = writerFactory.create(finalLang);
        Context cxt = ARQ.getContext().copy();

        try (OutputStream out = consoleOutSupplier.get()) {
            List<Var> vars = rowSet.getResultVars();
            Iterator<Binding> it = rowSet.getRdd().toLocalIterator();
            RowSet rs = RowSetStream.create(vars, it);
            writer.write(out, ResultSet.adapt(rs), cxt);
        }
    }
}
