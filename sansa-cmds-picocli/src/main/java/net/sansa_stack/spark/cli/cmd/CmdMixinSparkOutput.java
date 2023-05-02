package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.impl.RdfOutputConfig;
import picocli.CommandLine;

import java.util.List;

public class CmdMixinSparkOutput
    implements RdfOutputConfig
{
    @CommandLine.Option(names = { "-o", "--out-format" },
            description = "Output format. Default: ${DEFAULT-VALUE}")
    public String outFormat = null;

    @CommandLine.Option(names = { "--out-folder" },
            description = "Output folder")
    public String outFolder = null;

    @CommandLine.Option(names = { "--out-file" },
            description = "Output file; Merge of files created in out-folder")
    public String outFile = null;

    @CommandLine.Option(names = { "--out-overwrite" }, arity = "0",
            description = "Overwrite existing output files and/or folders")
    public boolean outOverwrite = false;

    @CommandLine.Option(names = { "--op", "--out-prefixes" },
            description = "Prefix sources for output. Subject to used prefix analysis. Default: ${DEFAULT-VALUE}",
            defaultValue = "rdf-prefixes/prefix.cc.2019-12-17.ttl")
    public List<String> outPrefixes = null;

    @CommandLine.Option(names = { "--oup", "--out-used-prefixes" },
            description = "Only for streaming to STDOUT. Number of records by which to defer RDF output for collecting used prefixes. Negative value emits all known prefixes. Default: ${DEFAULT-VALUE}",
            defaultValue = "100")
    public long deferOutputForUsedPrefixes;

    @Override
    public Long getPrefixOutputDeferCount() {
        return deferOutputForUsedPrefixes;
    }

    @Override
    public List<String> getPrefixSources() {
        return outPrefixes;
    }

    @Override
    public String getOutputFormat() {
        return outFormat;
    }

    @Override
    public String getPartitionFolder() {
        return outFolder;
    }

    @Override
    public String getTargetFile() {
        return outFile;
    }

    @Override
    public boolean isOverwriteAllowed() { return outOverwrite; }
}
