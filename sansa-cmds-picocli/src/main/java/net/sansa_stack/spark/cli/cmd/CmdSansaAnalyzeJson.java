package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaAnalyzeJsonImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "json",
description = "Analyze parsing of JSON")
public class CmdSansaAnalyzeJson
    extends CmdBase
    implements Callable<Integer>
    {
    @CommandLine.Mixin
    public CmdMixinSparkInput inputConfig = new CmdMixinSparkInput();

    @CommandLine.Mixin
    public CmdMixinSparkPostProcess postProcessConfig = new CmdMixinSparkPostProcess();

    @CommandLine.Parameters(arity = "1..n", description = "Input JSON file(s)")
    public List<String> inputFiles;

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @Override
    public Integer call() throws Exception {
        return CmdSansaAnalyzeJsonImpl.run(this);
    }
}
