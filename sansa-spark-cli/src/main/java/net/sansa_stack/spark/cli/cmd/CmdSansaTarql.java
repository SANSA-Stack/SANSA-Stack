package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaTarqlImpl;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.picocli.PicocliMixinCsvw;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "tarql",
        description = "Map one or more CSV files to RDF via a single SPARQL query",
        mixinStandardHelpOptions = true)
public class CmdSansaTarql
    extends CmdBase
    implements Callable<Integer>
{
    /* Just use spark's -Dspark.master=... option
    @Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster = null;
    */

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @CommandLine.Mixin
    public DialectMutable csvOptions = PicocliMixinCsvw.of(new DialectMutableImpl());

    @Option(names = { "--rq" }, description = "File with a SPARQL query (RDF Query)")
    public String queryFile = null;

    @Parameters(arity = "1..n", description = "CSV file(s)")
    public List<String> inputFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaTarqlImpl.run(this);
    }
}
