package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.io.rdf.output.RdfPostProcessingSettings;
import net.sansa_stack.spark.io.rdf.output.RdfPostProcessingSettingsBase;
import picocli.CommandLine;

public class CmdMixinSparkPostProcess
    implements RdfPostProcessingSettings
{
    @CommandLine.Option(names = { "--pretty" }, description = "Enables --sort, --unique and --optimize-prefixes")
    public void setPretty(boolean value) {
        this.unique = true;
        this.sort = true;
        this.optimizePrefixes = true;
    }

    @CommandLine.Option(names = { "-u", "--unique" },
            description = "Make quads unique")
    public boolean unique = false;

    @CommandLine.Option(names = { "--unique-partitions" },
            description = "Number of partitions to use for the unique operation")
    public Integer uniquePartitions;

    // Implement this at some point
    /*
    @CommandLine.Option(names = { "-d", "--drop-duplicates" },
            description = "Drop consecutively repeated items")
    public boolean dropDuplicates = false;
    */

    @CommandLine.Option(names = { "-s", "--sort" },
            description = "Sort data (component order is gspo)")
    public boolean sort = false;

    @CommandLine.Option(names = { "-r", "--reverse" },
            description = "Sort ascending (does nothing if --sort is not specified)")
    public boolean reverse = false;

    @CommandLine.Option(names = { "--sort-partitions" },
            description = "Number of partitions to use for the sort operation")
    public Integer sortPartitions;

    @CommandLine.Option(names = { "--optimize-prefixes" },
            description = "Only output used prefixes (requires additional scan of the data)")
    public boolean optimizePrefixes = false;


    @Override
    public Boolean getDistinct() {
        return unique;
    }

    @Override
    public Integer getDistinctPartitions() {
        return uniquePartitions;
    }

    @Override
    public Boolean getSort() {
        return sort;
    }

    @Override
    public Boolean getSortAscending() {
        return reverse;
    }

    @Override
    public Integer getSortPartitions() {
        return sortPartitions;
    }

    @Override
    public Boolean getOptimizePrefixes() {
        return optimizePrefixes;
    }
}
