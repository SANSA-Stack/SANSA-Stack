package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "ngs",
        description = "Named Graph Stream Commands",
        subcommands = {
                CmdSansaNgsSort.class,
                CmdSansaNgsQuery.class
        })
public class CmdSansaNgs {
}
