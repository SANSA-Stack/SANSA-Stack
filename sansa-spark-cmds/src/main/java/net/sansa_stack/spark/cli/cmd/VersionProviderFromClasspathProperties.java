package net.sansa_stack.spark.cli.cmd;

import picocli.CommandLine.IVersionProvider;

import java.io.InputStream;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;

/**
 * Implementation of picocli's {@link IVersionProvider} that reads the version string
 * from an entry of a properties file on the class path with a specific key.
 *
 * This class is suitable for copy&paste between projects
 * (or to put into a central place)
 *
 * Subclasses are then tied to a specific setup
 *
 * @author Claus Stadler
 *
 */
public abstract class VersionProviderFromClasspathProperties implements IVersionProvider {

    public abstract String getResourceName();
    public abstract Collection<String> getStrings(Properties properties);

    @Override
    public String[] getVersion() throws Exception {
        String resourceName = getResourceName();

        Properties properties = new Properties();
        try (InputStream in = Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(resourceName),
                "Resource not found: " + resourceName)) {
            properties.load(in);
        }

        String[] result = getStrings(properties).toArray(new String[0]);
        return result;
    }

}
