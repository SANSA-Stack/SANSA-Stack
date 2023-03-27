package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "rml",
description = "RML-related commands",
subcommands = {
        CmdSansaRmlToTarql.class
})
public class CmdSansaRmlParent {
}
