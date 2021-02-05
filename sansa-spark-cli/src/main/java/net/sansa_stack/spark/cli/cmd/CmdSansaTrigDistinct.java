package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.cmd.impl.CmdSansaTrigDistinctImpl;
import net.sansa_stack.spark.cli.cmd.impl.CmdSansaTrigQueryImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "distinct",
        description = "Merge data from multiple trig files to make it distinct")
public class CmdSansaTrigDistinct
        extends CmdBase
        implements Callable<Integer>
{
    @CommandLine.Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster;

    @CommandLine.Option(names = { "-o", "--out-format" },
            description = "Output format. Default: ${DEFAULT-VALUE}",
            defaultValue = "srj")
    public String outFormat = null;


    @CommandLine.Parameters(arity = "1..n", description = "Trig File")
    public List<String> trigFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaTrigDistinctImpl.run(this);
    }
}
