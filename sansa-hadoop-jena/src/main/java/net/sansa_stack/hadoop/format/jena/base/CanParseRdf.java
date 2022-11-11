package net.sansa_stack.hadoop.format.jena.base;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.rdf.model.Model;

import net.sansa_stack.hadoop.util.FileSystemUtils;

public interface CanParseRdf {
    /** This is currently a stub method - the final method should return an Iterator of
     * parse events, however this API only recently became public in Jena */
    Model parsePrefixes(InputStream inputStream, Configuration conf) throws IOException;

    default Model parsePrefixes(FileSystem fs, Path path, Configuration conf) throws IOException {
        try (InputStream in = FileSystemUtils.newInputStream(path, fs, conf)) {
            return parsePrefixes(in, conf);
        }
    }
}
