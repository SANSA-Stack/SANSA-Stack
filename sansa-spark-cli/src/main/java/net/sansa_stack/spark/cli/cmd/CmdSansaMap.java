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
