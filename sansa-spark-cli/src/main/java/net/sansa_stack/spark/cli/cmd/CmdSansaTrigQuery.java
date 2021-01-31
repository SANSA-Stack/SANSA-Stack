package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.cmd.impl.CmdSansaTrigQueryImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
@Command(name = "trig",
        description = "Run a special SPARQL query on a trig file")
public class CmdSansaTrigQuery
    extends CmdBase
    implements Callable<Integer>
{
    @Option(names = { "-m", "--spark-master" },
            description = "Spark master. Defaults to ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster;

    @Option(names = { "-o", "--out-format" },
            description = "Output format. Defaults to ${DEFAULT-VALUE}",
            defaultValue = "srj")
    public String outFormat = null;

    @Option(names = { "--rq" }, description = "File with a SPARQL query (RDF Query)")
    public String queryFile = null;

    @Parameters(arity = "1", description = "Trig File")
    public String trigFile;


    @Override
    public Integer call() throws Exception {
        return CmdSansaTrigQueryImpl.run(this);
    }
}
