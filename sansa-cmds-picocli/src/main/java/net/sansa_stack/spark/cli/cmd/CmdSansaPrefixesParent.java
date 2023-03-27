package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "prefixes",
    description = "Process prefixes of RDF files",
    subcommands = {
            CmdSansaPrefixesHead.class, CmdSansaPrefixesUsed.class, CmdSansaPrefixesOptimize.class
    })
public class CmdSansaPrefixesParent {
}
