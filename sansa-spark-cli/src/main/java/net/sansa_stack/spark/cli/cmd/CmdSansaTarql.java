package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaTarqlImpl;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "tarql",
        description = "Map one or more CSV files to RDF via a single SPARQL query")
public class CmdSansaTarql
    extends CmdBase
    implements Callable<Integer>
{
    /* Just use spark's -Dspark.master=... option
    @Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster = null;
    */

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @Option(names = { "--rq" }, description = "File with a SPARQL query (RDF Query)")
    public String queryFile = null;

    @Parameters(arity = "1..n", description = "CSV file(s)")
    public List<String> inputFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaTarqlImpl.run(this);
    }
}