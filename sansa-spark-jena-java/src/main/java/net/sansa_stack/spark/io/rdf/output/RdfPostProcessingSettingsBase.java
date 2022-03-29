package net.sansa_stack.spark.io.rdf.output;

public class RdfPostProcessingSettingsBase
    implements RdfPostProcessingSettingsMutable
{
    protected Boolean distinct;
    protected Integer distinctPartitions;

    protected Boolean sort;
    protected Boolean sortAscending;
    protected Integer sortPartitions;

    protected Boolean optimizePrefixes;

    public RdfPostProcessingSettingsBase() {
    }

    public RdfPostProcessingSettingsBase(
            Boolean distinct, Integer distinctPartitions,
            Boolean sort, Boolean sortAscending,
            Integer sortPartitions,
            Boolean optimizePrefixes) {
        this.distinct = distinct;
        this.distinctPartitions = distinctPartitions;
        this.sort = sort;
        this.sortAscending = sortAscending;
        this.sortPartitions = sortPartitions;
        this.optimizePrefixes = optimizePrefixes;
    }

    @Override
    public Boolean getDistinct() {
        return distinct;
    }

    @Override
    public Integer getDistinctPartitions() {
        return distinctPartitions;
    }

    @Override
    public Boolean getSort() {
        return sort;
    }

    @Override
    public Boolean getSortAscending() {
        return sortAscending;
    }

    @Override
    public Integer getSortPartitions() {
        return sortPartitions;
    }

    @Override
    public Boolean getOptimizePrefixes() {
        return optimizePrefixes;
    }

    @Override
    public void setDistinct(Boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public void setDistinctPartitions(Integer distinctPartitions) {
        this.distinctPartitions = distinctPartitions;
    }

    @Override
    public void setSort(Boolean sort) {
        this.sort = sort;
    }

    @Override
    public void setSortAscending(Boolean sortAscending) {
        this.sortAscending = sortAscending;
    }

    @Override
    public void setSortPartitions(Integer sortPartitions) {
        this.sortPartitions = sortPartitions;
    }

    @Override
    public void setOptimizePrefixes(Boolean optimizePrefixes) {
        this.optimizePrefixes = optimizePrefixes;
    }

    /*
    public static class Builder
        extends RdfPostProcessingSettings
    {
        public Boolean getDistinct() {
            return distinct;
        }

        public Integer getDistinctPartitions() {
            return distinctPartitions;
        }

        public Boolean getSort() {
            return sort;
        }

        public Boolean getSortAscending() {
            return sortAscending;
        }

        public Integer getSortPartitions() {
            return sortPartitions;
        }

        public Boolean getOptimizePrefixes() {
            return optimizePrefixes;
        }


        public RdfPostProcessingSettings build() {
            return new RdfPostProcessingSettings(
                    distinct, distinctPartitions,
                    sort, sortAscending, sortPartitions,
                    optimizePrefixes);
        }
    }
     */
}
