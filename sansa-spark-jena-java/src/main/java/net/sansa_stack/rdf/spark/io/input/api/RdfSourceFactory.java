package net.sansa_stack.rdf.spark.io.input.api;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.Lang;


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
        return get(sourceStr, (Lang) null);
    }

    default RdfSource get(String sourceStr, Lang lang) {
        Path path = new Path(sourceStr);
        return get(path, null, lang);
    }

    default RdfSource get(String sourceStr, FileSystem fileSystem) {
        Path path = new Path(sourceStr);
        return get(path, fileSystem, null);
    }

    default RdfSource get(Path path, FileSystem fileSystem, Lang lang) {
        try {
            return create(path, fileSystem, lang);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    RdfSource create(Path path, FileSystem fileSystem, Lang lang) throws Exception;
}
