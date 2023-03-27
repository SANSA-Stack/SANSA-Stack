package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "endpoint",
        description = "Start an HTTP SPARQL endpoint service on a given RDF triples file or Spark database")
public class CmdSansaEndpoint
    extends CmdBase
    implements Callable<Integer> {
    @Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster;

    @ArgGroup(exclusive = true, multiplicity = "1")
    public DatasetArgs dataset;

    @Option(names = { "-p", "--port" },
            description = "HTTP port. Default: ${DEFAULT-VALUE}",
            defaultValue = "9999")
    public int port = 9999;

    @Option(names = { "--query-engine" },
            description = "the SPARQL backend ('Ontop', 'Sparqlify')")
    public String queryEngine = null;

    public static class DatasetArgs {
        @ArgGroup(exclusive = false, multiplicity = "1", heading = "Arguments for simple RDF triples file.")
        public FreshDatasetArgs freshDatasetArgs;

        @ArgGroup(exclusive = false, multiplicity = "1", heading = "Arguments for pre-loaded dataset.")
        public PreloadedDatasetArgs preloadedDatasetArgs;
    }

    public static class FreshDatasetArgs {
        @Parameters(arity = "1..n", description = "RDF triples file")
        public List<String> triplesFile;

        @Option(names = { "--distinct", "--make-distinct" },
                description = "Start with making all triples across all input files distinct. Default: ${DEFAULT-VALUE}",
                defaultValue = "false")
        public boolean makeDistinct;
    }

    public static class PreloadedDatasetArgs {
        @Option(names = { "--database" },
                required = true,
                description = "the database name")
        public String database = null;

        @Option(names = { "--mappings-file" },
                required = true,
                description = "the R2RML mappings file ")
        public String mappingsFile = null;
    }

    @Override
    public Integer call() throws Exception {
        // This command loads the implementation only lazily because Ontop / Sparqlify are quite
        // heavy weight dependencies which may not be included in all environments

        // A better solution might be be to prevent registration of this command at all if the implementation
        // is not available

        // return CmdSansaEndpointImpl.run(this);
        Class<?> clz = Class.forName("net.sansa_stack.spark.cli.impl.CmdSansaEndpointImpl");
        Method m = clz.getDeclaredMethod("run", this.getClass());
        Object r = m.invoke(this);
        return (Integer)r;
    }
}
