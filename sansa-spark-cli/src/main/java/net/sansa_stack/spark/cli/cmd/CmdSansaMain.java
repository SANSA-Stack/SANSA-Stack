package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "sansa",
        versionProvider = VersionProviderSansa.class,
        description = "SANSA Command Line Tooling",
        subcommands = {
        CmdSansaTrigQuery.class
})
public class CmdSansaMain {
    public static boolean debugMode = false;
}
