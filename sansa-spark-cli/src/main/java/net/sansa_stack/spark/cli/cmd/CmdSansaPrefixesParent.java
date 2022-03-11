package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "prefixes",
        subcommands = {
                CmdSansaPrefixesHead.class, CmdSansaPrefixesUsed.class
        })
public class CmdSansaPrefixesParent {
}
