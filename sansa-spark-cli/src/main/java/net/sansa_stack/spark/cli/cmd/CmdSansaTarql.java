package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.cmd.impl.CmdSansaTarqlImpl;
import net.sansa_stack.spark.cli.cmd.impl.CmdSansaTrigQueryImpl;
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

    @Option(names = { "--of", "--out-format" },
            description = "Output format. Default: ${DEFAULT-VALUE}",
            defaultValue = "srj")
    public String outFormat = null;

    @CommandLine.Option(names = { "--out-folder" },
            description = "Output folder")
    public String outFolder = null;

    @CommandLine.Option(names = { "--out-file" },
            description = "Output file; Merge of files created in out-folder")
    public String outFile = null;

    @Option(names = { "--rq" }, description = "File with a SPARQL query (RDF Query)")
    public String queryFile = null;

    @Parameters(arity = "1..n", description = "CSV file(s)")
    public List<String> trigFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaTarqlImpl.run(this);
    }
}
