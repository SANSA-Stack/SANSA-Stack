package net.sansa_stack.hadoop.core.plugin;

import org.aksw.jenax.reprogen.core.JenaPluginUtils;
import org.apache.jena.sys.JenaSubsystemLifecycle;

import net.sansa_stack.hadoop.core.ProbeStats;
import net.sansa_stack.hadoop.core.Stats2;

public class JenaPluginSansaHadoop
    implements JenaSubsystemLifecycle
{
    @Override
    public void start() {
        JenaPluginUtils.registerResourceClasses(Stats2.class, ProbeStats.class);
    }

    @Override
    public void stop() {
        // Nothing to do
    }
}
