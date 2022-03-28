package net.sansa_stack.spark.io.rdf.output;

public interface RdfPostProcessingSettings {
    Boolean getDistinct();
    Integer getDistinctPartitions();
    Boolean getSort();
    Boolean getSortAscending();
    Integer getSortPartitions();
    Boolean getOptimizePrefixes();

    default void copyInto(RdfPostProcessingSettingsMutable dest) {
        dest.setDistinct(getDistinct());
        dest.setDistinctPartitions(getDistinctPartitions());
        dest.setSort(getSort());
        dest.setSortAscending(getSortAscending());
        dest.setSortPartitions(getSortPartitions());
        dest.setOptimizePrefixes(dest.getOptimizePrefixes());
    }
}
