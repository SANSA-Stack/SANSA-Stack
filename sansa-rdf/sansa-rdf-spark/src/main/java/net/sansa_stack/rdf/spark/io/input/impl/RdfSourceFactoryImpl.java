package net.sansa_stack.rdf.spark.io.input.impl;

import java.io.InputStream;
import java.util.Objects;

import org.aksw.jena_sparql_api.rx.RDFDataMgrEx;
import org.aksw.jena_sparql_api.rx.entity.EntityInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.spark.sql.SparkSession;

import net.sansa_stack.rdf.spark.io.input.api.RdfSource;
import net.sansa_stack.rdf.spark.io.input.api.RdfSourceFactory;

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


    public RdfSource create(String sourceStr) throws Exception {
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        return create(sourceStr, fileSystem);
    }


    @Override
    public RdfSource create(String sourceStr, FileSystem fileSystem) throws Exception {
        Path path = new Path(sourceStr);

        return create(path, fileSystem);
    }

    @Override
    public RdfSource create(Path path, FileSystem fileSystem) throws Exception {
        Path resolvedPath = fileSystem.resolvePath(path);

        EntityInfo entityInfo;
        try (InputStream in = fileSystem.open(resolvedPath)) {
             entityInfo = RDFDataMgrEx.probeEntityInfo(in, RDFDataMgrEx.DEFAULT_PROBE_LANGS);
        }

        Lang lang = RDFLanguages.contentTypeToLang(entityInfo.getContentType());

        Objects.requireNonNull(lang, "Could not obtain lang for " + entityInfo.getContentType() + " from " + path);

        return new RdfSourceImpl(sparkSession, resolvedPath, lang);
    }
}
