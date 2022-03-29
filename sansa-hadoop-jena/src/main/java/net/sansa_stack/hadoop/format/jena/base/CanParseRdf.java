package net.sansa_stack.hadoop.format.jena.base;

import com.esotericsoftware.kryo.io.Input;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.rdf.model.Model;

import java.io.IOException;
import java.io.InputStream;

public interface CanParseRdf {
    /** This is currently a stub method - the final method should return an Iterator of
     * parse events, however this part is currently not public in Jena */
    Model parsePrefixes(InputStream inputStream, Configuration conf) throws IOException;

    default Model parsePrefixes(FileSystem fs, Path path, Configuration conf) throws IOException {
        try (InputStream in = fs.open(path)) {
            return parsePrefixes(in, conf);
        }
    }

}
