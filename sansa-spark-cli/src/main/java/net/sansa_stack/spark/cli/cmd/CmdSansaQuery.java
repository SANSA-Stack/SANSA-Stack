package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaQueryImpl;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "query",
        description = "Map one or more RDF files to RDF via a list of SPARQL queries",
        mixinStandardHelpOptions = true)
public class CmdSansaQuery
        extends CmdBase
        implements Callable<Integer> {

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @CommandLine.Mixin
    public CmdMixinSparkPostProcess postProcessConfig = new CmdMixinSparkPostProcess();

    @Parameters(arity = "1..n", description = "query file(s)")
    public List<String> queryFiles;

    @Option(names = {"--iriasgiven"}, arity = "0", description = "Use an alternative IRI() implementation that is non-validating but fast")
    public boolean useIriAsGiven = false;

    @Option(names = {"--no-dag"}, arity = "0", description = "Do not perform DAG scheduling of SPARQL queries to group common operations")
    public boolean noDagScheduling = false;

    @Override
    public Integer call() throws Exception {
        return CmdSansaQueryImpl.run(this);
    }
}
