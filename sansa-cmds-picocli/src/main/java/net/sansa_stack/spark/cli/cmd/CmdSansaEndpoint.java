package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaEndpointImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ArgGroup;

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
        return CmdSansaEndpointImpl.run(this);
    }
}
