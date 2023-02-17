package net.sansa_stack.spark.cli.main;

import net.sansa_stack.spark.cli.cmd.CmdBase;
import net.sansa_stack.spark.cli.cmd.CmdSansaParent;
import org.aksw.commons.util.derby.DerbyUtils;
import org.aksw.commons.util.exception.ExceptionUtilsAksw;
import org.apache.jena.sys.JenaSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class MainCliSansaSpark {

    private static final Logger logger = LoggerFactory.getLogger(CmdSansaParent.class);

    // Required to init registries such as result set formats
    static {
        DerbyUtils.disableDerbyLog();
        JenaSystem.init();
    }

    public static void main(String[] args) {
        int exitCode = mainCore(args);
        System.exit(exitCode);
    }

    public static int mainCore(String[] args) {
        int result = new CommandLine(new CmdSansaParent())
                .setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
                    CmdBase cmd = commandLine.getCommand();
                    boolean debugMode = cmd.debugMode;
                    if (debugMode) {
                        ExceptionUtilsAksw.rethrowIfNotBrokenPipe(ex);
                    } else {
                        ExceptionUtilsAksw.forwardRootCauseMessageUnless(ex, logger::error, ExceptionUtilsAksw::isBrokenPipeException);
                    }
                    return 0;
                })
                .execute(args);
        return result;
    }

}
