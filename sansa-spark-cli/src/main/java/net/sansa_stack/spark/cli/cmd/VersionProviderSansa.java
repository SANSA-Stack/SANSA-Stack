package net.sansa_stack.spark.cli.cmd;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class VersionProviderSansa
    extends VersionProviderFromClasspathProperties
{
    @Override public String getResourceName() { return "sansa.properties"; }
    @Override public Collection<String> getStrings(Properties p) { return Arrays.asList(
            p.get("sansa.version") + " built at " + p.get("sansa.build.timestamp")
    ); }

}
