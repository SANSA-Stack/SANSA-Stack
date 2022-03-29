package net.sansa_stack.spark.io.rdf.input.api;

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

    default RdfSourceFromResource get(String sourceStr) {
        return get(sourceStr, (Lang) null);
    }

    default RdfSourceFromResource get(String sourceStr, Lang lang) {
        Path path = new Path(sourceStr);
        return get(path, null, lang);
    }

    default RdfSourceFromResource get(String sourceStr, FileSystem fileSystem) {
        Path path = new Path(sourceStr);
        return get(path, fileSystem, null);
    }

    default RdfSourceFromResource get(Path path, FileSystem fileSystem, Lang lang) {
        try {
            return create(path, fileSystem, lang);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    RdfSourceFromResource create(Path path, FileSystem fileSystem, Lang lang) throws Exception;

    /** Return a collection to which RdfSources can be added from which a union rdd
     * can be obtained */
    RdfSourceCollection newRdfSourceCollection();
}
