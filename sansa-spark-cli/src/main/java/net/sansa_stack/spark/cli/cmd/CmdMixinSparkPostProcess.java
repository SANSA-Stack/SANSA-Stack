package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine;

public class CmdMixinSparkPostProcess {
    @CommandLine.Option(names = { "-u", "--unique" },
            description = "Make quads unique")
    public boolean unique = false;

    @CommandLine.Option(names = { "-d", "--drop-duplicates" },
            description = "Drop consecutively repeated items")
    public boolean dropDuplicates = false;

    @CommandLine.Option(names = { "-s", "--sort" },
            description = "Enable sorting of graphs by their IRI")
    public boolean sort = false;

    @CommandLine.Option(names = { "-r", "--reverse" },
            description = "Sort descending (requires --sort)")
    public boolean reverse = false;

    @CommandLine.Option(names = { "--repartition" },
            description = "Number of partitions to use for grouping / sorting. '0' or negative values disable repartitioning")
    public int numPartitions = 0;

}
