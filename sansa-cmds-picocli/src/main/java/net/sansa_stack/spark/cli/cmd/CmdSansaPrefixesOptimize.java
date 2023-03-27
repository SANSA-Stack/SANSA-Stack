package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaPrefixesOptimizeImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "optimize",
        description = "Optimize prefixes")
public class CmdSansaPrefixesOptimize
        extends CmdBase
        implements Callable<Integer>
{
    /*
    @CommandLine.Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster;
     */

    @CommandLine.Mixin
    public CmdMixinSparkInput inputConfig = new CmdMixinSparkInput();

    @CommandLine.Parameters(arity = "1..n", description = "Input RDF file(s)")
    public List<String> inputFiles;

    @CommandLine.Option(names = { "-n" },
            description = "The target prefix collection size; deafult ${DEFAULT-VALUE}",
            defaultValue = "1000")
    public int targetSize;

    @Override
    public Integer call() throws Exception {
        return CmdSansaPrefixesOptimizeImpl.run(this);
    }
}
