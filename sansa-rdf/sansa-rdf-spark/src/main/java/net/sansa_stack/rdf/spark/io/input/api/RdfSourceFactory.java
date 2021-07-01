package net.sansa_stack.rdf.spark.io.input.api;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Turn source references into sources of RDF data.
 *
 * Note: This interface uses {@link FileSystem}. It may be possible
 * to abstract this with a {@link java.nio.file.FileSystem} at a later stage.
 *
 * @author raven
 *
 */
public interface RdfSourceFactory {

    default RdfSource get(String sourceStr) {
        try {
            return create(sourceStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    RdfSource create(String sourceStr) throws Exception;
    RdfSource create(String sourceStr, FileSystem fileSystem) throws Exception;
    RdfSource create(Path path, FileSystem fileSystem) throws Exception;
}
