package net.sansa_stack.spark.cli.cmd;

import java.util.List;
import java.util.concurrent.Callable;

import net.sansa_stack.spark.cli.impl.CmdSansaRmlToTarqlImpl;
import picocli.CommandLine;

@CommandLine.Command(name = "to-tarql", description = "Convert RML/CSV mappings to Tarql/SPARQL queries")
public class CmdSansaRmlToTarql
    extends CmdBase
    implements Callable<Integer>
{
    @CommandLine.Parameters(arity = "1..n", description = "Input RDF file(s)")
    public List<String> inputFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaRmlToTarqlImpl.run(this);
    }
}
