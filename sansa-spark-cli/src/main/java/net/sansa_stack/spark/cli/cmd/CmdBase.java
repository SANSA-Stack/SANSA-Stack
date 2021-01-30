package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine.Option;

public class CmdBase {
    @Option(names = { "-X" }, description = "Debug mode; enables full stack traces")
    public boolean debugMode = false;

}
