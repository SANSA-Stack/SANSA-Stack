package net.sansa_stack.spark.io.rdf.output;

public interface RdfPostProcessingSettingsMutable
    extends RdfPostProcessingSettings
{
    void setDistinct(Boolean distinct);
    void setDistinctPartitions(Integer distinctPartitions);
    void setSort(Boolean sort);
    void setSortAscending(Boolean sortAscending);
    void setSortPartitions(Integer sortPartitions);
    void setOptimizePrefixes(Boolean optimizePrefixes);

    default void copyFrom(RdfPostProcessingSettings src) {
        setDistinct(src.getDistinct());
        setDistinctPartitions(src.getDistinctPartitions());
        setSort(src.getSort());
        setSortAscending(src.getSortAscending());
        setSortPartitions(src.getSortPartitions());
        setOptimizePrefixes(src.getOptimizePrefixes());
    }
}
