package net.sansa_stack.spark.io.rdf.input.impl;

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

import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;

/**
 * Implementation of a source factory based on spark/hadoop.
 *
 * @author raven
 *
 */
public class RdfSourceFactoryImpl
    implements RdfSourceFactory
{
    protected SparkSession sparkSession;
    // protected FileSystem fileSystem;

    public RdfSourceFactoryImpl(SparkSession sparkSession) {
        super();
        this.sparkSession = sparkSession;
    }

    public static RdfSourceFactory from(SparkSession sparkSession) {
        return new RdfSourceFactoryImpl(sparkSession);
    }

    @Override
    public RdfSource create(Path path, FileSystem fileSystem, Lang lang) throws Exception {

        if (fileSystem == null) {
            Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
            fileSystem = FileSystem.get(hadoopConf);
        }

        Path resolvedPath = fileSystem.resolvePath(path);

        if (lang == null) {
            EntityInfo entityInfo;
            try (InputStream in = fileSystem.open(resolvedPath)) {
                entityInfo = RDFDataMgrEx.probeEntityInfo(in, RDFDataMgrEx.DEFAULT_PROBE_LANGS);
            }
            lang = RDFLanguages.contentTypeToLang(entityInfo.getContentType());

            Objects.requireNonNull(lang, "Could not obtain lang for " + entityInfo.getContentType() + " from " + path);
        }

        return new RdfSourceImpl(sparkSession, resolvedPath, lang);
    }
}
