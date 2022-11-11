package net.sansa_stack.spark.io.rdf.output;

import org.aksw.commons.io.util.StdIo;
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx;
import org.apache.hadoop.fs.Path;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;

import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class RddRdfWriterSettings<SELF extends RddRdfWriterSettings> {
    protected Path partitionFolder;
    protected Path targetFile;

    protected boolean useCoalesceOne;

    /* Only applicable if a on outputFile is specified */
    protected boolean deletePartitionFolderAfterMerge;
    protected PrefixMapping globalPrefixMapping;
    protected RDFFormat outputFormat;

    // protected Object saveMode; // SaveMode saveMode;//mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
    protected boolean allowOverwriteFiles;
    protected boolean useElephas;

    protected boolean partitionsAsIndependentFiles;

    /** Whether to convert quads to triples if a triple-based output format is requested */
    protected boolean mapQuadsToTriplesForTripleLangs;

    protected RdfPostProcessingSettingsMutable postProcessingSettings = new RdfPostProcessingSettingsBase();

    /**
     * Only for console output: Instead of writing tuples out immediatly,
     * collect up to this number of tuples in order to derive the used prefixes.
     * Upon reaching this threshold, print out all seen prefixes and emit the held-back data
     * as well as any further data immediately */
    protected long deferOutputForUsedPrefixes = 0;

    protected Supplier<OutputStream> consoleOutSupplier = StdIo::openStdOutWithCloseShield;

    public boolean isMapQuadsToTriplesForTripleLangs() {
        return mapQuadsToTriplesForTripleLangs;
    }

    protected SELF self() {
        return (SELF)this;
    }

    // TODO Probably we should copy / clone prefixes
    public SELF configureFrom(RddRdfWriterSettings<?> other) {
        this.partitionFolder = other.partitionFolder;
        this.targetFile = other.targetFile;
        this.useCoalesceOne = other.useCoalesceOne;
        this.deletePartitionFolderAfterMerge = other.deletePartitionFolderAfterMerge;
        this.globalPrefixMapping = other.globalPrefixMapping;
        this.outputFormat = other.outputFormat;
        this.allowOverwriteFiles = other.allowOverwriteFiles;
        this.useElephas = other.useElephas;
        this.partitionsAsIndependentFiles = other.partitionsAsIndependentFiles;
        this.mapQuadsToTriplesForTripleLangs = other.mapQuadsToTriplesForTripleLangs;
        this.deferOutputForUsedPrefixes = other.deferOutputForUsedPrefixes;
        this.consoleOutSupplier = other.consoleOutSupplier;

        this.postProcessingSettings.copyFrom(other.postProcessingSettings);
        return self();
    }

    /**
     * Whether to convert quads to triples if a triple-based output format is requested
     * Jena by default discards any quad outside of the default graph when writing to a triple format.
     * Setting this flag to true will map each quad in a named graph to the default graph.
     */
    public SELF setMapQuadsToTriplesForTripleLangs(boolean mapQuadsToTriplesForTripleLangs) {
        this.mapQuadsToTriplesForTripleLangs = mapQuadsToTriplesForTripleLangs;
        return self();
    }

    public boolean isUseCoalesceOne() {
        return useCoalesceOne;
    }

    public void setUseCoalesceOne(boolean useCoalesceOne) {
        this.useCoalesceOne = useCoalesceOne;
    }

    public boolean isDeletePartitionFolderAfterMerge() {
        return deletePartitionFolderAfterMerge;
    }

    public SELF setDeletePartitionFolderAfterMerge(boolean deletePartitionFolderAfterMerge) {
        this.deletePartitionFolderAfterMerge = deletePartitionFolderAfterMerge;
        return self();
    }

    public PrefixMapping getGlobalPrefixMapping() {
        return globalPrefixMapping;
    }

    public Path getPartitionFolder() {
        return partitionFolder;
    }

    public SELF setPartitionFolder(Path partitionFolder) {
        this.partitionFolder = partitionFolder;
        return self();
    }

    public SELF setPartitionFolder(String partitionFolder) {
        return setPartitionFolder(partitionFolder == null ? null : new Path(partitionFolder));
    }

    public Path getTargetFile() {
        return targetFile;
    }

    public SELF setTargetFile(Path targetFile) {
        this.targetFile = targetFile;
        return self();
    }

    public SELF setTargetFile(String targetFile) {
        return setTargetFile(targetFile == null ? null : new Path(targetFile));
    }

    /**
     * Set a prefix mapping to be used "globally" across all partitions.
     *
     * @param globalPrefixMapping
     * @return
     */
    public SELF setGlobalPrefixMapping(PrefixMapping globalPrefixMapping) {
        this.globalPrefixMapping = globalPrefixMapping;
        return self();
    }

    public SELF setGlobalPrefixMapping(Map<String, String> globalPrefixMap) {
        PrefixMapping pm = new PrefixMappingImpl();
        pm.setNsPrefixes(globalPrefixMap);
        setGlobalPrefixMapping(pm);
        return self();
    }

    public RDFFormat getOutputFormat() {
        return outputFormat;
    }

    public SELF setOutputFormat(RDFFormat format) {
        this.outputFormat = format;
        return self();
    }

    /** Raises an exception if the format is not found */
    public SELF setOutputFormat(String formatName) {
    	RDFFormat fmt = formatName == null
    			? null
    			: Optional.ofNullable(RDFLanguagesEx.findRdfFormat(formatName))
	                .orElseThrow(() -> new IllegalArgumentException("Unknown format: " + formatName));
        return setOutputFormat(fmt);
    }

    public RDFFormat getFallbackOutputFormat() {
        return outputFormat;
    }
    
    public boolean isAllowOverwriteFiles() {
        return allowOverwriteFiles;
    }

    public SELF setAllowOverwriteFiles(boolean allowOverwriteFiles) {
        this.allowOverwriteFiles = allowOverwriteFiles;
        return self();
    }

    public boolean isUseElephas() {
        return useElephas;
    }

    public SELF setUseElephas(boolean useElephas) {
        this.useElephas = useElephas;
        return self();
    }

    public boolean isPartitionsAsIndependentFiles() {
        return partitionsAsIndependentFiles;
    }

    public SELF setPartitionsAsIndependentFiles(boolean partitionsAsIndependentFiles) {
        this.partitionsAsIndependentFiles = partitionsAsIndependentFiles;
        return self();
    }

    public SELF setDeferOutputForUsedPrefixes(long deferOutputForUsedPrefixes) {
        this.deferOutputForUsedPrefixes = deferOutputForUsedPrefixes;
        return self();
    }

    /** If neither partition folder nor targe file is set the output goes to the console */
    public boolean isConsoleOutput() {
        return partitionFolder == null && targetFile == null;
    }

    public SELF setConsoleOutput() {
        this.partitionFolder = null;
        this.targetFile = null;
        return self();
    }

    public SELF setConsoleOutSupplier(Supplier<OutputStream> consoleOutSupplier) {
        this.consoleOutSupplier = consoleOutSupplier;
        return self();
    }

    public Supplier<OutputStream> getConsoleOutSupplier() {
        return consoleOutSupplier;
    }


    public RdfPostProcessingSettingsMutable getPostProcessingSettings() {
        return postProcessingSettings;
    }

    public SELF setPostProcessingSettings(RdfPostProcessingSettingsMutable postProcessingSettings) {
        this.postProcessingSettings = postProcessingSettings;
        return self();
    }
    
}
