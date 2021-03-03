package net.sansa_stack.rdf.spark.io;

/*
public class PartitionWriterSpark {
    protected boolean useCoalesceOne;

    protected Path partitionFolder;

    protected Path targetFile;
    protected JavaRDD<?> rdd;

    protected PrefixMapping globalPrefixMapping;
    protected RDFFormat outputFormat;

    protected Object saveMode; // SaveMode saveMode;//mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
    protected boolean exitOnError; // exitOnError: Boolean = false

    public void run() {

    }


    // TODO This method should go to a common util class
    def mergeFolder(outFile: java.nio.file.Path, srcFolder: java.nio.file.Path, pattern: String): Unit = {
        val partPaths = FileUtils.listPaths(srcFolder, pattern)
        java.util.Collections.sort(partPaths, (a: java.nio.file.Path, b: java.nio.file.Path) => a.getFileName.toString.compareTo(b.getFileName.toString))
        logger.info("Creating file %s by merging %d files from %s".format(
                outFile.toString, partPaths.size, srcFolder.toString))

        // val sw = Stopwatch.createStarted
        val merger = FileMerger.create(outFile, partPaths)
        merger.addProgressListener(self => logger.info(
                "Write progress for %s: %.2f%%".format(outFile.getFileName.toString, self.getProgress * 100.0)))
        merger.run
    }

    /**
     * Save the RDD to a single file.
     * Underneath invokes [[JenaDatasetWriter#saveToFolder]] and merges
     * the set of files created by it.
     * See [[JenaDatasetWriter#saveToFolder]] for supported formats.
     *
     * @param outFile
     * @param prefixMapping
     * @param rdfFormat
     * @param outFolder The folder for the part files; may be null.
     * @param mode
     * @param exitOnError
     * /
    def saveToFile(outFile: String,
                   prefixMapping: PrefixMapping,
                   rdfFormat: RDFFormat,
                   outFolder: String,
                   mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
                   exitOnError: Boolean = false): Unit = {

        val outFilePath = Paths.get(outFile).toAbsolutePath
        val outFileFileName = outFilePath.getFileName.toString
        val outFolderPath =
        if (outFolder == null) outFilePath.resolveSibling(outFileFileName + "-parts")
        else Paths.get(outFolder).toAbsolutePath

        saveToFolder(outFolderPath.toString, prefixMapping, rdfFormat, mode, exitOnError)
        mergeFolder(outFilePath, outFolderPath, "part*")
    }

    /**
     * Save the data in Trig/Turtle or its sub-formats (n-quads/n-triples) format.
     * If prefixes should be written out then they have to provided as an argument to
     * the prefixMapping parameter.
     * Prefix mappings are broadcasted to and processed in a .mapPartition operation.
     * If the prefixMapping is non-empty then the first part file written out contains them.
     * No other partition will write out prefixes.
     *
     * @param path the folder into which the file(s) will be written to
     * @param mode the expected behavior of saving the data to a data source
     * @param exitOnError whether to stop if an error occurred
     * /
    def saveToFolder(path: String,
                     prefixMapping: PrefixMapping,
                     rdfFormat: RDFFormat,
                     mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
                     exitOnError: Boolean = false): Unit = {

        val fsPath = new Path(path)
        val fs = fsPath.getFileSystem(quads.sparkContext.hadoopConfiguration)

        val doSave = if (fs.exists(fsPath)) {
            mode match {
                case SaveMode.Overwrite =>
                    fs.delete(fsPath, true)
                    true
                case SaveMode.ErrorIfExists =>
                    sys.error(s"Given path $path already exists!")
                    if (exitOnError) sys.exit(1)
                    false
                case SaveMode.Ignore => false
                case _ =>
                    throw new IllegalStateException(s"Unsupported save mode $mode ")
            }
        } else {
            true
        }

      import scala.collection.JavaConverters._

        val prefixStr: String =
        if (prefixMapping != null && !prefixMapping.hasNoMappings) {
            makeString(prefixMapping, RDFFormat.TURTLE_PRETTY)
        } else {
            null
        }


        val prefixMappingBc = quads.sparkContext.broadcast(prefixMapping)

        // save only if there was no failure with the path before
        if (doSave) {
            val rdfFormatStr = rdfFormat.toString

            val dataBlocks = quads
                    .mapPartitions(p => {
            if (p.hasNext) {
                // Look up the string here in order to avoid having to serialize the RDFFormat object
                val rdfFormat = RDFLanguagesEx.findRdfFormat(rdfFormatStr)

                val out = new PipedOutputStream() // throws IOException
                val in = new PipedInputStream(out, 8 * 1024)
                val rawWriter = StreamRDFWriter.getWriterStream(out, rdfFormat, null)

                // Retain blank nodes as given
                if (rawWriter.isInstanceOf[WriterStreamRDFBase]) {
                    WriterStreamRDFBaseUtils.setNodeToLabel(rawWriter.asInstanceOf[WriterStreamRDFBase], SyntaxLabels.createNodeToLabelAsGiven())
                }

                // Set the writer's prefix map without writing them out
                val writer = WriterStreamRDFBaseWrapper.wrapWithFixedPrefixes(
                        prefixMappingBc.value, rawWriter.asInstanceOf[WriterStreamRDFBase])

                val thread = new Thread(() => {
                try {
                    writer.start
                    while (p.hasNext) {
                        val ds: JenaDataset = p.next
                        StreamRDFOps.sendDatasetToStream(ds.asDatasetGraph(), writer)
                    }
                    writer.finish
                    out.flush
                } finally {
                    out.close
                }
              });
                thread.start();
                // Collections.singleton(baos.toString("UTF-8").trim).iterator().asScala
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                        .lines().iterator().asScala
            } else {
                Iterator()
            }
          })

            // If there are prefixes then serialize them into their own partition and prepend them to all
            // the other serialized data partitions.
            // Note that this feature is unstable as it relies on spark retaining order of partitions (which so far it does)
            val allBlocks: RDD[String] =
            if (prefixStr != null) {
                val prefixRdd = quads.sparkContext.parallelize(Seq(prefixStr))
                prefixRdd.union(dataBlocks)
            } else {
                dataBlocks
            }

            allBlocks
                    .saveAsTextFile(path)
        }
    }

    public static void saveUsingElephas(String path) {
        // determine language based on file extension
        val lang = RDFLanguages.filenameToLang(path)

        // unknown format
        if (!RDFLanguages.isQuads(lang)) {
            throw new IllegalArgumentException(s"couldn't determine syntax for RDF quads based on file extension in given path $path")
        }

        // N-Triples can be handle efficiently via file splits
        if (lang == Lang.NQUADS) {
            saveAsNQuadsFile(path)
        } else { // others can't
            val sc = quads.sparkContext

            val confHadoop = sc.hadoopConfiguration

            quads.zipWithIndex().map{case (k, v) => (v, k)}
          .saveAsNewAPIHadoopFile(path, classOf[LongWritable], classOf[QuadWritable], classOf[QuadsOutputFormat[LongWritable]], confHadoop)
        }
    }
}
*/