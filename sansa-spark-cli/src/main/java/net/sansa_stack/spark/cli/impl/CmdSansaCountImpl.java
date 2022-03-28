package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaCount;

import java.io.IOException;

public class CmdSansaCountImpl {

    public static int run(CmdSansaCount cmd) throws IOException {

        new SimpleSparkCmdTemplate<>("Sansa Count Triples/Quads", cmd.inputConfig, cmd.inputFiles) {
            @Override
            protected void process() {
                long result;
                if (rdfSources.usesQuads()) {
                    result = rdfSources.asQuads().count();
                } else {
                    result = rdfSources.asTriples().count();
                }
                System.out.println(result);
            }
        }.call();

        return 0; // exit code
    }

}
