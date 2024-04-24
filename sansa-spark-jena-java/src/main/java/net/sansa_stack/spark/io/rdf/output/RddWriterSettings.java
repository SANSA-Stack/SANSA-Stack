package net.sansa_stack.spark.io.rdf.output;

import org.aksw.commons.io.util.StdIo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.OutputStream;
import java.util.function.Supplier;

public class RddWriterSettings<SELF extends RddWriterSettings> {
    protected Configuration hadoopConfiguration;
    protected Path partitionFolder;
    protected FileSystem partitionFolderFs;

    protected Path targetFile;
    protected FileSystem targetFileFs;

    protected boolean useCoalesceOne;

    /* Only applicable if a on outputFile is specified */
    protected boolean deletePartitionFolderAfterMerge;

    // protected Object saveMode; // SaveMode saveMode;//mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
    protected boolean allowOverwriteFiles;

    protected boolean partitionsAsIndependentFiles;

    protected RdfPostProcessingSettingsMutable postProcessingSettings = new RdfPostProcessingSettingsBase();

    protected Supplier<OutputStream> consoleOutSupplier = StdIo::openStdOutWithCloseShield;

    protected SELF self() {
        return (SELF)this;
    }

    // TODO Probably we should copy / clone prefixes
    public SELF configureFrom(RddWriterSettings<?> other) {
        this.hadoopConfiguration = other.hadoopConfiguration;
        this.partitionFolder = other.partitionFolder;
        this.partitionFolderFs = other.partitionFolderFs;
        this.targetFile = other.targetFile;
        this.targetFileFs = other.targetFileFs;
        this.useCoalesceOne = other.useCoalesceOne;
        this.deletePartitionFolderAfterMerge = other.deletePartitionFolderAfterMerge;
        this.allowOverwriteFiles = other.allowOverwriteFiles;
        this.partitionsAsIndependentFiles = other.partitionsAsIndependentFiles;
        this.consoleOutSupplier = other.consoleOutSupplier;

        this.postProcessingSettings.copyFrom(other.postProcessingSettings);
        return self();
    }

    public Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    public SELF setHadoopConfiguration(Configuration hadoopConfiguration) {
        this.hadoopConfiguration = hadoopConfiguration;
        return self();
    }

    public boolean isUseCoalesceOne() {
        return useCoalesceOne;
    }

    public SELF setUseCoalesceOne(boolean useCoalesceOne) {
        this.useCoalesceOne = useCoalesceOne;
        return self();
    }

    public boolean isDeletePartitionFolderAfterMerge() {
        return deletePartitionFolderAfterMerge;
    }

    public SELF setDeletePartitionFolderAfterMerge(boolean deletePartitionFolderAfterMerge) {
        this.deletePartitionFolderAfterMerge = deletePartitionFolderAfterMerge;
        return self();
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

    public FileSystem getPartitionFolderFs() {
        return partitionFolderFs;
    }

    public SELF setPartitionFolderFs(FileSystem partitionFolderFs) {
        this.partitionFolderFs = partitionFolderFs;
        return self();
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

    public FileSystem getTargetFileFs() {
        return targetFileFs;
    }

    public SELF setTargetFileFs(FileSystem targetFileFs) {
        this.targetFileFs = targetFileFs;
        return self();
    }

    public boolean isAllowOverwriteFiles() {
        return allowOverwriteFiles;
    }

    public SELF setAllowOverwriteFiles(boolean allowOverwriteFiles) {
        this.allowOverwriteFiles = allowOverwriteFiles;
        return self();
    }

    public boolean isPartitionsAsIndependentFiles() {
        return partitionsAsIndependentFiles;
    }

    public SELF setPartitionsAsIndependentFiles(boolean partitionsAsIndependentFiles) {
        this.partitionsAsIndependentFiles = partitionsAsIndependentFiles;
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
