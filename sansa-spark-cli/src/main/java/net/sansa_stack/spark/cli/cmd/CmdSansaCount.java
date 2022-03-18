package net.sansa_stack.spark.cli.cmd;


import net.sansa_stack.spark.cli.impl.CmdSansaCountImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "count",
        description = "Count triples/quads")
public class CmdSansaCount
    implements Callable<Integer>
{
    @CommandLine.Mixin
    public CmdMixinSparkInput inputConfig = new CmdMixinSparkInput();

    @CommandLine.Mixin
    public CmdMixinSparkPostProcess postProcessConfig = new CmdMixinSparkPostProcess();

    @CommandLine.Parameters(arity = "1..n", description = "Input RDF file(s)")
    public List<String> inputFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaCountImpl.run(this);
    }
}
