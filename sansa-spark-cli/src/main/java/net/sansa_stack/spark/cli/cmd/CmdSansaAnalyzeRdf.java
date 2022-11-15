package net.sansa_stack.spark.cli.cmd;


import java.util.List;
import java.util.concurrent.Callable;

import net.sansa_stack.spark.cli.impl.CmdSansaAnalyzeRdfImpl;
import picocli.CommandLine;

@CommandLine.Command(name = "analyze",
        description = "Analyze parsing of RDF triples/quads")
public class CmdSansaAnalyzeRdf
        extends CmdBase
        implements Callable<Integer>
{
    @CommandLine.Mixin
    public CmdMixinSparkInput inputConfig = new CmdMixinSparkInput();

    @CommandLine.Mixin
    public CmdMixinSparkPostProcess postProcessConfig = new CmdMixinSparkPostProcess();

    @CommandLine.Parameters(arity = "1..n", description = "Input RDF file(s)")
    public List<String> inputFiles;

    // TODO Replace with a non-spark mixin that only captures output file and (rdf) format
    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @Override
    public Integer call() throws Exception {
        return CmdSansaAnalyzeRdfImpl.run(this);
    }
}
