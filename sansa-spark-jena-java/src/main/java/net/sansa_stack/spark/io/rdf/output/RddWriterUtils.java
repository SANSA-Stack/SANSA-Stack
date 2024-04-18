package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.hadoop.output.jena.base.OutputUtils;
import net.sansa_stack.spark.io.common.HadoopOutputFormat;
import org.aksw.commons.io.util.FileMerger;
import org.aksw.commons.io.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** Utilities common to (but not limited to) Rdf and RowSet output */
public class RddWriterUtils {

    private static final Logger logger = LoggerFactory.getLogger(RddWriterUtils.class);

    public static Configuration buildBaseConfiguration(RDD<?> rdd) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(rdd.context());
        Configuration baseConf = sparkContext.hadoopConfiguration();
        Configuration result = new Configuration(baseConf);

        int numPartitions =  rdd.getNumPartitions();
        OutputUtils.setSplitCount(result, numPartitions);
        return result;
    }

    public static <T> JavaPairRDD<Long, T> toPairRdd(JavaRDD<T> rdd) {
        JavaPairRDD<Long, T> result = rdd
                .mapToPair(v -> new Tuple2<>(0l, v));
        return result;
    }

    public static void save(JavaPairRDD<?, ?> pairRdd, HadoopOutputFormat entry, Path path, Configuration hadoopConfiguration) {
        pairRdd.saveAsNewAPIHadoopFile(path.toString(),
                entry.getKeyClass(),
                entry.getValueClass(),
                entry.getFormatClass(),
                hadoopConfiguration);
    }

    /*
    public static RddWriterSettings<?> applyDefaultFileSystems(RddWriterSettings<?> cxt) throws IOException {
        RddWriterSettings<?> tmp = prepare(cxt);


        return cxt;
    }
     */

    public static RddWriterSettings<?> prepare(RddWriterSettings<?> cxt, Configuration fallback) throws IOException {
        Configuration hadoopConfiguration = cxt.getHadoopConfiguration();
        Configuration finalHadoopConfiguration = hadoopConfiguration != null
                ? hadoopConfiguration
                : fallback;

        Path targetFile = cxt.getTargetFile();
        Path partitionFolder = cxt.getPartitionFolder();
        FileSystem partitionFolderFs = cxt.getPartitionFolderFs();
        Path effPartitionFolder = partitionFolder;
        boolean allowOverwriteFiles = cxt.isAllowOverwriteFiles();

/*
        val outFilePath = Paths.get(outFile).toAbsolutePath
        val outFileFileName = outFilePath.getFileName.toString
        val outFolderPath =
        if (outFolder == null) outFilePath.resolveSibling(outFileFileName + "-parts")
        else Paths.get(outFolder).toAbsolutePath

        saveToFolder(outFolderPath.toString, prefixMapping, rdfFormat, mode, exitOnError)
        mergeFolder(outFilePath, outFolderPath, "part*")
*/

        if (targetFile != null && partitionFolder == null) {
            // targetFile.getFileSystem(hadoopConfiguration);
            // java.nio.file.Path targetFilePath = Paths.get(targetFile.toUri());
            String targetFileName = cxt.getTargetFile().getName();
            // java.nio.file.Path effPartitionFolderPath = path.resolveSibling(targetFileName + "-parts");
            effPartitionFolder = new Path(cxt.getTargetFile().toUri().resolve("part-" + targetFileName));
            // effPartitionFolder = new Path(effPartitionFolderPath.toString());
        }


        if (partitionFolderFs == null) {
            partitionFolderFs = effPartitionFolder.getFileSystem(finalHadoopConfiguration);
        }

        if (partitionFolderFs.exists(effPartitionFolder)) {
            if (allowOverwriteFiles) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("Attempting to safely remove existing file/folder: %s", effPartitionFolder));
                }
                // partitionFolderFs.delete(effPartitionFolder, true);
                safeDeletePartitionFolder(partitionFolderFs, effPartitionFolder, finalHadoopConfiguration);

                if (partitionFolderFs.exists(effPartitionFolder)) {
                    String msg = String.format("Could not safely remove partition folder '%s' because non-hadoop files exist. Please delete manually.", effPartitionFolder);
//                    if (logger.isInfoEnabled()) {
//                        logger.info(msg);
//                    }
                    throw new RuntimeException(msg);
                }
            } else {
                throw new IllegalArgumentException("Folder already exists: " + effPartitionFolder);
            }
        }

        // URI targetFileUri = null;
        FileSystem targetFileFs = null;
        if (targetFile != null) {
            targetFileFs = targetFile.getFileSystem(finalHadoopConfiguration);
            if (targetFileFs.exists(targetFile)) {
                if (allowOverwriteFiles) {
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format("Removing existing file: %s", targetFile));
                    }
                    targetFileFs.delete(targetFile, false);
                } else {
                    throw new IllegalArgumentException("File already exists: " + targetFile);
                }
            }
        }


        // JavaRDD<T> effectiveRdd = rdd.map(x -> (T)x);
//        JavaRDD effectiveRdd = getEffectiveRdd(postProcessingSettings);
//
//        if (useCoalesceOne) {
//            effectiveRdd = effectiveRdd.coalesce(1);
//        }
        return new RddWriterSettings<>()
            .setTargetFileFs(targetFileFs)
            .setTargetFile(targetFile)
            .setPartitionFolderFs(partitionFolderFs)
            .setPartitionFolder(effPartitionFolder)
            .setHadoopConfiguration(finalHadoopConfiguration)
            .setAllowOverwriteFiles(cxt.isAllowOverwriteFiles())
            .setDeletePartitionFolderAfterMerge(cxt.isDeletePartitionFolderAfterMerge())
            .setConsoleOutSupplier(cxt.getConsoleOutSupplier())
            .setPostProcessingSettings(cxt.getPostProcessingSettings())
            .setUseCoalesceOne(cxt.isUseCoalesceOne())
            ;
    }

    public static void postProcess(RddWriterSettings<?> cxt) throws IOException {
        Configuration hadoopConfiguration = cxt.getHadoopConfiguration();
        Path targetFile = cxt.getTargetFile();
        FileSystem targetFileFs = cxt.getTargetFileFs();
        Path partitionFolder = cxt.getPartitionFolder();
        FileSystem partitionFolderFs = cxt.getPartitionFolderFs();
        Path effPartitionFolder = partitionFolder;
        boolean allowOverwriteFiles = cxt.isAllowOverwriteFiles();

        if (targetFile != null) {
            if (!(partitionFolderFs instanceof LocalFileSystem) || !(targetFileFs instanceof LocalFileSystem)) {
                throw new IllegalArgumentException("Merge currently only supports local file system");
            } else {
                java.nio.file.Path nioFolder = Paths.get(partitionFolder.toString());
                java.nio.file.Path nioFile = Paths.get(targetFile.toString());
                mergeFolder(nioFile, nioFolder, "part-*", null);
            }

            if (cxt.isDeletePartitionFolderAfterMerge()) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("Removing temporary output folder: %s", partitionFolder));
                }
                cxt.getPartitionFolderFs().delete(partitionFolder, true);
            }
        }
    }

    /** Merge all files in the given srcFolder into outFile (uses java nio abstraction which can be backed by hadoop paths) */
    public static void mergeFolder(
            java.nio.file.Path outFile,
            java.nio.file.Path srcFolder,
            String pattern,
            Comparator<? super java.nio.file.Path> pathComparator) throws IOException {
        if (pathComparator == null) {
            pathComparator = (java.nio.file.Path a, java.nio.file.Path b) -> a.getFileName().toString().compareTo(b.getFileName().toString());
        }
        List<java.nio.file.Path> partPaths = FileUtils.listPaths(srcFolder, pattern);
        Collections.sort(partPaths, pathComparator);
        logger.info(String.format("Creating file %s by merging %d files from %s",
                outFile, partPaths.size(), srcFolder));

        // val sw = Stopwatch.createStarted
        FileMerger merger = FileMerger.create(outFile, partPaths);
        merger.addProgressListener(self -> logger.info(
                String.format("Write progress for %s: %.2f%%",
                        outFile.getFileName(),
                        self.getProgress() * 100.0)));
        merger.run();
    }

    /**
     * This method first checks that all top-level files in the partition folder belong to hadoop.
     * If this is the case then a single recursive delete is made.
     *
     * Note that concurrent modifications could still cause those files to be deleted.
     * Raises [@link DirectoryNotEmptyException} if not all files can be removed.
     * If the directory does not exist then it is ignored.
     */
    public static void safeDeletePartitionFolder(FileSystem fs, Path folderPath, Configuration conf) throws IOException {
        // The constants in the line below are defined in FileOutputFormat - but proteceted!
        String baseName = Optional.ofNullable(conf.get("mapreduce.output.basename")).orElse("part");
        if (baseName == null) {
            baseName = "part";
        }

        if (baseName.isBlank()) {
            if (logger.isWarnEnabled()) {
                // An empty base name prefixes every file
                // Even if there are multiple white spaces is seems more like a misconfiguration
                logger.warn("Deletion is disabled for blank base names as a safety measure");
            }
            return;
        }

        try {
            // Amazing how slow listFiles is for local directories...
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(folderPath, false);

            boolean allDeletable = true;
            Path path = null;
            while (it.hasNext()) {
                LocatedFileStatus status = it.next();
                path = status.getPath();

                String fileName = path.getName();

                // Create the effective name for further processing:
                // Strip the name of a leading dot - those files are considered to hold check sums
                String effName = fileName.replaceAll("^\\.", "");

                allDeletable = allDeletable &&
                        (effName.startsWith(baseName) || effName.equals("_temporary") || effName.equals("_SUCCESS"));

                if (logger.isDebugEnabled()) {
                    logger.debug("Can delete " + path + ": " + allDeletable);
                }

                if (!allDeletable) {
                    break;
                }
            }

            if (allDeletable) {
                fs.delete(folderPath, true);
            } else {
                throw new DirectoryNotEmptyException("Safe delete refused to delete non-hadoop file: " + path);
            }
        } catch (FileNotFoundException e) {
            // Nothing to delete
        }
    }

    /*
    public static void validateOutFolder(Path path, Configuration conf, boolean deleteIfExists) throws IOException {
        // Path fsPath = new Path(path);
        FileSystem fs = FileSystem.get(path.toUri(), conf);

        if (fs.exists(path)) {
            if (deleteIfExists) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("Removing temporary output folder: %s", path));
                }
                fs.delete(path, true);
            } else {
                throw new IllegalArgumentException("File already exists: " + fs);
            }
        }
    }
    */
}
