package net.sansa_stack.spark.io.rdf.input.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.aksw.commons.util.entity.EntityInfo;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFromResource;

/**
 * Implementation of a source factory based on spark/hadoop.
 *
 * @author raven
 *
 */
public class RdfSourceFactoryImpl
    implements RdfSourceFactory
{
    private static final Logger logger = LoggerFactory.getLogger(RdfSourceFactoryImpl.class);

    protected SparkSession sparkSession;
    // protected FileSystem fileSystem;

    public RdfSourceFactoryImpl(SparkSession sparkSession) {
        super();
        this.sparkSession = sparkSession;
    }

    /** Use {@link RdfSourceFactories#of(SparkSession)} instead. */
    @Deprecated
    public static RdfSourceFactory from(SparkSession sparkSession) {
        return RdfSourceFactories.of(sparkSession);
    }

    @Override
    public RdfSourceFromResource create(Path path, FileSystem fileSystem, Lang lang) throws Exception {

        if (fileSystem == null) {
            Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
            fileSystem = FileSystem.get(hadoopConf);
        }

        Path resolvedPath = fileSystem.resolvePath(path);

        if (lang == null) {
            lang = probeLang(resolvedPath, fileSystem);
        }

        logger.info("Creating RDF Source: " + path + " -> " + lang);

        return new RdfSourceFromResourceImpl(sparkSession, resolvedPath, lang);
    }

    @Override
    public RdfSourceCollection newRdfSourceCollection() {
        return new RdfSourceCollectionImpl(sparkSession);
    }

    public static Lang probeLang(Path path, FileSystem fileSystem) throws IOException {
        if (fileSystem == null) {
            fileSystem = Objects.requireNonNull(getDefaultFileSystem(), "Failed to obtain the default file system");
        }

        EntityInfo entityInfo;
        try (InputStream in = fileSystem.open(path)) {
            entityInfo = RDFDataMgrEx.probeEntityInfo(in, RDFDataMgrEx.DEFAULT_PROBE_LANGS);
        }

        Lang result = null;

        if (entityInfo != null) {
            result = RDFLanguages.contentTypeToLang(entityInfo.getContentType());
            Objects.requireNonNull(result, "Could not obtain lang for " + entityInfo.getContentType() + " from " + path);
        }

        return result;
    }

    public static FileSystem getDefaultFileSystem() throws IOException {
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");

        FileSystem result = FileSystem.get(conf);
        return result;
    }
}
