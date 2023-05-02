package net.sansa_stack.spark.cli.cmd;

import java.util.List;
import java.util.concurrent.Callable;

import net.sansa_stack.spark.cli.impl.CmdSansaPrefixesHeadImpl;
import picocli.CommandLine;

@CommandLine.Command(name = "head",
        description = "Collect the prefixes at the beginning of the given sources")
public class CmdSansaPrefixesHead
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

    @Override
    public Integer call() throws Exception {
        return CmdSansaPrefixesHeadImpl.run(this);
    }
}
