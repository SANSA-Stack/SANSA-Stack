package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.cmd.impl.CmdSansaTrigQueryImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.concurrent.Callable;
@Command(name = "query",
        description = "Run a special SPARQL query on a trig file")
public class CmdSansaTrigQuery
    extends CmdBase
    implements Callable<Integer>
{
    /* Just use spark's -Dspark.master=... option
    @Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster = null;
    */

    @Option(names = { "-o", "--out-format" },
            description = "Output format. Default: ${DEFAULT-VALUE}",
            defaultValue = "srj")
    public String outFormat = null;

    @Option(names = { "--rq" }, description = "File with a SPARQL query (RDF Query)")
    public String queryFile = null;

    @Parameters(arity = "1..n", description = "Trig File")
    public List<String> trigFiles;

    @Option(names = { "--distinct", "--make-distinct" },
            description = "Start with making all quads across all input files distinct; groups all named graphs by name. Default: ${DEFAULT-VALUE}",
            defaultValue = "false")
    public boolean makeDistinct;

    @Override
    public Integer call() throws Exception {
        return CmdSansaTrigQueryImpl.run(this);
    }
}
