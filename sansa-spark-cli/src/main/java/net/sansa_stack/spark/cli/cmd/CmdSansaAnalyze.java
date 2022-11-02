package net.sansa_stack.spark.cli.cmd;


import java.util.List;
import java.util.concurrent.Callable;

import net.sansa_stack.spark.cli.impl.CmdSansaAnalyzeImpl;
import picocli.CommandLine;

@CommandLine.Command(name = "analyze",
        description = "Analyze triples/quads")
public class CmdSansaAnalyze
        extends CmdBase
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
        return CmdSansaAnalyzeImpl.run(this);
    }
}
