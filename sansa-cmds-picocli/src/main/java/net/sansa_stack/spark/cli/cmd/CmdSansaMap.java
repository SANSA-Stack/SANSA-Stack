package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaMapImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "map",
        description = "Map, postprocess and write out RDF data")
public class CmdSansaMap
        extends CmdBase
        implements Callable<Integer>
{
    /*
    @CommandLine.Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster;
     */



    @CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
    public MapOperation mapOperation;

    public static class MapOperation {
        // @CommandLine.Option(names = { "--rq" }, description = "Query string or filename with a SPARQL query (RDF Query)")
        // public String querySource = null;

        @CommandLine.Option(names = { "-g", "--graph" }, description = "Graph to map all triples into")
        public String graphName = null;

        @CommandLine.Option(names = { "--dg", "--default-graph" }, description = "Map all graphs into the default graph")
        public Boolean defaultGraph = null;
    }

    @CommandLine.Mixin
    public CmdMixinSparkInput inputConfig = new CmdMixinSparkInput();

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @CommandLine.Mixin
    public CmdMixinSparkPostProcess postProcessConfig = new CmdMixinSparkPostProcess();

    @CommandLine.Parameters(arity = "1..n", description = "Input RDF file(s)")
    public List<String> inputFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaMapImpl.run(this);
    }
}
