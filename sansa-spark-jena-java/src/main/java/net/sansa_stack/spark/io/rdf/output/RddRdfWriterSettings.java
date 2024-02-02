package net.sansa_stack.spark.io.rdf.output;

import org.aksw.jenax.arq.util.lang.RDFLanguagesEx;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class RddRdfWriterSettings<SELF extends RddRdfWriterSettings>
    extends RddWriterSettings<SELF>
{
    protected PrefixMapping globalPrefixMapping;
    protected RDFFormat outputFormat;

    // protected boolean useElephas;

    /** Whether to convert quads to triples if a triple-based output format is requested */
    protected boolean mapQuadsToTriplesForTripleLangs;

    /**
     * Only for console output: Instead of writing tuples out immediatly,
     * collect up to this number of tuples in order to derive the used prefixes.
     * Upon reaching this threshold, print out all seen prefixes and emit the held-back data
     * as well as any further data immediately */
    protected long deferOutputForUsedPrefixes = 0;

    public boolean isMapQuadsToTriplesForTripleLangs() {
        return mapQuadsToTriplesForTripleLangs;
    }

    protected SELF self() {
        return (SELF)this;
    }

    /**
     * Pass this object to a consumer. Useful to conditionally configure this object
     * without breaking the fluent chain:
     *
     * <pre>
     *    rdd.configureSave().mutate(self -> { if (condition) { self.setX(); }}).run();
     * </pre>
     *
     * @param action
     * @return
     */
    public SELF mutate(Consumer<? super SELF> action) {
        SELF self = self();
        action.accept(self);
        return self;
    }

    // TODO Probably we should copy / clone prefixes
    public SELF configureFrom(RddRdfWriterSettings<?> other) {
        super.configureFrom(other);

        this.globalPrefixMapping = other.globalPrefixMapping;
        this.outputFormat = other.outputFormat;
        // this.useElephas = other.useElephas;
        this.deferOutputForUsedPrefixes = other.deferOutputForUsedPrefixes;
        this.mapQuadsToTriplesForTripleLangs = other.mapQuadsToTriplesForTripleLangs;
        this.deferOutputForUsedPrefixes = other.deferOutputForUsedPrefixes;

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

    public PrefixMapping getGlobalPrefixMapping() {
        return globalPrefixMapping;
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

    /*
    public boolean isUseElephas() {
        return useElephas;
    }

    public SELF setUseElephas(boolean useElephas) {
        this.useElephas = useElephas;
        return self();
    }
    */

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
}
