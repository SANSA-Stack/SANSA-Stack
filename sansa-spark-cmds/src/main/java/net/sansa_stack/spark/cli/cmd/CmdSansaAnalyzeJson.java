package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaAnalyzeCsvImpl;
import net.sansa_stack.spark.cli.impl.CmdSansaAnalyzeJsonImpl;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.picocli.PicocliMixinCsvw;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.ArrayList;
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
