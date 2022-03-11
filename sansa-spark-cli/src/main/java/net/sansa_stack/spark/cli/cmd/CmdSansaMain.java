package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sansa",
        versionProvider = VersionProviderSansa.class,
        description = "SANSA Command Line Tool",
        subcommands = {
        CmdSansaPrefixesParent.class, CmdSansaMap.class, CmdSansaTarql.class, CmdSansaNgs.class
})
public class CmdSansaMain extends CmdBase {

    @Option(names = { "-h", "--help" }, usageHelp = true)
    public boolean help = false;

    @Option(names = { "-v", "--version" }, versionHelp = true)
    public boolean version = false;

}
