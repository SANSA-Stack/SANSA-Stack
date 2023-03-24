package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaQueryImpl;
import org.aksw.jenax.arq.picocli.CmdMixinArq;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // @Option(names = {"--iriasgiven"}, arity = "0", description = "Use an alternative IRI() implementation that is non-validating but fast")
    // public boolean useIriAsGiven = false;
    @Option(names = {"--standard-iri"}, arity = "0", description = "Instead of treating 'IRIs as given' use Jena's standard IRI() implementation that is much slower due to validation and locking")
    public boolean standardIri = false;

    @Option(names = {"--hide-warnings"}, arity = "0", description = "Do not warn when creating invalid node values", defaultValue = "false")
    public boolean hideWarnings = false;

    @Option(names = {"--dag"}, arity = "0", description = "Add rdd.cache() nodes for certain common sub expressions in the sparql algebra. Experimental; usually makes things slower.")
    public boolean dagScheduling = false;

    @CommandLine.Mixin
    public CmdMixinArq arqConfig = new CmdMixinArq();

    @Override
    public Integer call() throws Exception {
        return CmdSansaQueryImpl.run(this);
    }
}
