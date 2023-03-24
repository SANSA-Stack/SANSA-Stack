package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine.Command;

@Command(name = "analyze",
    description = "Analyze parallel parsing of files",
    subcommands = { CmdSansaAnalyzeRdf.class, CmdSansaAnalyzeCsv.class, CmdSansaAnalyzeJson.class }
)
public class CmdSansaAnalyzeParent {
}
