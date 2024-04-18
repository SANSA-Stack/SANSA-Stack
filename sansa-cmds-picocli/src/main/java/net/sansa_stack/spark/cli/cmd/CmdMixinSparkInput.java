package net.sansa_stack.spark.cli.cmd;

import net.sansa_stack.spark.cli.util.RdfInputConfig;
import picocli.CommandLine;

public class CmdMixinSparkInput
    implements RdfInputConfig
{
    @CommandLine.Option(names = { "-i", "--in-format" },
            description = "Input format")
    public String inFormat = null;


    @Override
    public String getInputFormat() {
        return inFormat;
    }
}
