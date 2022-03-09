package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.CmdSansaNgsSortImpl;
import net.sansa_stack.spark.cli.impl.CmdSansaSortImpl;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "sort",
        description = "Merge data from multiple trig files to make it distinct")
public class CmdSansaNgsSort
        extends CmdBase
        implements Callable<Integer>
{
    /*
    @CommandLine.Option(names = { "-m", "--spark-master" },
            description = "Spark master. Default: ${DEFAULT-VALUE}",
            defaultValue = "local[*]")
    public String sparkMaster;
     */

    /*
    @CommandLine.Option(names = { "--out-file" },
            description = "Output file; Merge of files created in out-folder")
    public String outFile = null;
    */

    @CommandLine.Mixin
    public CmdMixinSparkOutput outputConfig = new CmdMixinSparkOutput();

    @CommandLine.Option(names = { "-d", "--distinct" },
            description = "Make quads distinct")
    public boolean distinct = false;

    @CommandLine.Option(names = { "-s", "--sort" },
            description = "Enable sorting of graphs by their IRI")
    public boolean sort = false;

    @CommandLine.Option(names = { "-r", "--reverse" },
            description = "Sort descending")
    public boolean reverse = false;

    @CommandLine.Option(names = { "--repartition" },
            description = "Number of partitions to use for grouping / sorting. '0' or negative values disable repartitioning")
    public int numPartitions = 0;


    @CommandLine.Parameters(arity = "1..n", description = "Trig File(s)")
    public List<String> inputFiles;

    @Override
    public Integer call() throws Exception {
        return CmdSansaNgsSortImpl.run(this);
    }
}
