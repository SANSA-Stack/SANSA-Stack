package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.cmd.impl.CmdSansaTrigMergeImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "merge",
        description = "Merge data from multiple trig files to make it distinct")
public class CmdSansaTrigMerge
        extends CmdBase
        implements Callable<Integer>
{
    /*
    @CommandLine.Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster;
     */

    @CommandLine.Option(names = { "-o", "--out-format" },
            description = "Output format. Default: ${DEFAULT-VALUE}",
            defaultValue = "trig/blocks")
    public String outFormat = null;

    @CommandLine.Option(names = { "--out-folder" },
            description = "Output folder")
    public String outFolder = null;

    @CommandLine.Option(names = { "--out-file" },
            description = "Output file; Merge of files created in out-folder")
    public String outFile = null;

    @CommandLine.Option(names = { "--op", "--out-prefixes" },
            description = "Prefix sources for output. Subject to used prefix analysis. Default: ${DEFAULT-VALUE}",
            defaultValue = "rdf-prefixes/prefix.cc.2019-12-17.ttl")
    public List<String> outPrefixes = null;

    @CommandLine.Option(names = { "--oup", "--out-used-prefixes" },
            description = "Number of records by which to defer RDF output for used prefix analysis. Negative value emits all prefixes. Default: ${DEFAULT-VALUE}",
            defaultValue = "100")
    public long deferOutputForUsedPrefixes;

    @CommandLine.Option(names = { "-d", "--distinct" },
            description = "Make quads distinct")
    public boolean distinct = false;

    @CommandLine.Option(names = { "-s", "--sort" },
            description = "Enable sorting of graphs by their IRI")
    public boolean sort = false;

    @CommandLine.Option(names = { "--repartition" },
            description = "Number of partitions to use for grouping / sorting. '0' or negative values disable repartitioning")
    public int numPartitions = 0;


    @CommandLine.Parameters(arity = "1..n", description = "Trig File(s)")
    public List<String> trigFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaTrigMergeImpl.run(this);
    }
}
