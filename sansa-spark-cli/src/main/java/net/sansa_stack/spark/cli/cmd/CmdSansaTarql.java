package net.sansa_stack.spark.cli.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;
import org.aksw.commons.model.csvw.picocli.PicocliMixinCsvw;

import net.sansa_stack.spark.cli.impl.CmdSansaTarqlImpl;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "tarql",
        description = "Map one or more CSV files to RDF via a single SPARQL query",
        mixinStandardHelpOptions = true)
public class CmdSansaTarql
    extends CmdBase
    implements Callable<Integer> {
    /* Just use spark's -Dspark.master=... option
    @Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster = null;
    */

    /*
    @CommandLine.ArgGroup(validate = false, heading = "CSV/TSV options")
    public CsvOptions csvOptions;

    @CommandLine.ArgGroup(validate = false, heading = "Output options")
    public OutputOptions outputOptions;

    public static class CsvOptions {
        @CommandLine.Mixin
        public DialectMutable csvOptions = PicocliMixinCsvw.of(new DialectMutableImpl());
    }

    public static class OutputOptions {
    }
*/

    @CommandLine.Mixin
    public DialectMutable csvOptions = PicocliMixinCsvw.of(new DialectMutableImpl());

    // @Option(names = { "--force-alloc-names" }, description = "Allocate column names even if headers are present; default: ${DEFAULT-VALUE}", defaultValue="false")
    // public boolean forceAllocColumnNames = false;

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @CommandLine.Mixin
    public CmdMixinSparkPostProcess postProcessConfig = new CmdMixinSparkPostProcess();

    @Option(names = { "-t", "--tabs" }, description = "Separators are tabs; default: ${DEFAULT-VALUE}", defaultValue="false")
    public boolean tabs = false;

    @Parameters(arity = "1..n", description = "tarql query file following by one or more csv file")
    public List<String> inputFiles;

    @Option(names = { "--ntriples" }, arity="0", description = "Tarql compatibility flag; turns any quad/triple based output to nquads/ntriples")
    public boolean ntriples = false;
    
    @Option(names = { "--header-naming" }, arity="1", description = "Which column names to use. Allowed values: 'row', 'excel'. Numerics such as '0', '1' number with that offset. If there are no header rows then 'row' is treated as 'excel'. Column names are unqiue, first name takes precedence.", defaultValue = "row")
    public List<String> columnNamingSchemes = new ArrayList<>();    
    
    @Override
    public Integer call() throws Exception {
        return CmdSansaTarqlImpl.run(this);
    }
}
