package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "trig",
        description = "Trig Related Commands",
        subcommands = {
                CmdSansaTrigMerge.class,
                CmdSansaTrigQuery.class
        })
public class CmdSansaTrig {
}
