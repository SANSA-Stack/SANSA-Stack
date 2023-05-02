package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaCount;

public class CmdSansaCountImpl {

    public static int run(CmdSansaCount cmd) throws Exception {

        new SimpleSparkCmdRdfTemplate<>("Sansa Count Triples/Quads", cmd.inputConfig, cmd.inputFiles) {
            @Override
            protected void process()  throws Exception {
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
