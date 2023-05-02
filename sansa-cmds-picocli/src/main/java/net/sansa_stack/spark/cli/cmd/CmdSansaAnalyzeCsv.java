package net.sansa_stack.spark.cli.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.picocli.PicocliMixinCsvw;

import net.sansa_stack.spark.cli.impl.CmdSansaAnalyzeCsvImpl;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(name = "csv",
description = "Analyze parsing of CSV")
public class CmdSansaAnalyzeCsv
    extends CmdBase
    implements Callable<Integer>
    {
    @CommandLine.Mixin
    public CmdMixinSparkInput inputConfig = new CmdMixinSparkInput();

    @CommandLine.Mixin
    public CmdMixinSparkPostProcess postProcessConfig = new CmdMixinSparkPostProcess();

    @CommandLine.Parameters(arity = "1..n", description = "Input CSV file(s)")
    public List<String> inputFiles;

    @CommandLine.Mixin
    public DialectMutable csvOptions = PicocliMixinCsvw.of(new DialectMutableImpl());

    @Option(names = { "-t", "--tabs" }, description = "Separators are tabs; default: ${DEFAULT-VALUE}", defaultValue="false")
    public boolean tabs = false;

    @Option(names = { "--header-naming" }, arity="1", description = "Which column names to use. Allowed values: 'row', 'excel'. Numerics such as '0', '1' number with that offset. If there are no header rows then 'row' is treated as 'excel'. Column names are unqiue, first name takes precedence.", defaultValue = "row")
    public List<String> columnNamingSchemes = new ArrayList<>();

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @Override
    public Integer call() throws Exception {
        return CmdSansaAnalyzeCsvImpl.run(this);
    }
}
