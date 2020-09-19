package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

class RelationalAlgebraOperations
{
    private final boolean supportsSortInSetOperator;
    private final Aggregation aggregation;
    private final JoinSupport join;
    private final SortSpecification sort;
    private final RelationalAlgebraOperation union;
    private final RelationalAlgebraOperation unionAll;
    private final Values values;
    
    RelationalAlgebraOperations(@JsonProperty("allow_set_operators_with_sort") final Boolean supportsSortInSetOperator, @JsonProperty("aggregation") final Aggregation aggregation, @JsonProperty("join") final JoinSupport join, @JsonProperty("sort") final SortSpecification sort, @JsonProperty("union") final RelationalAlgebraOperation union, @JsonProperty("union_all") final RelationalAlgebraOperation unionAll, @JsonProperty("values") final Values values) {
        this.supportsSortInSetOperator = (supportsSortInSetOperator != null && supportsSortInSetOperator);
        this.aggregation = aggregation;
        this.join = join;
        this.sort = sort;
        this.union = union;
        this.unionAll = unionAll;
        this.values = values;
    }
    
    public boolean supportsSortInSetOperator() {
        return this.supportsSortInSetOperator;
    }
    
    public Aggregation getAggregation() {
        return this.aggregation;
    }
    
    public Values getValues() {
        return this.values;
    }
    
    public RelationalAlgebraOperation getUnion() {
        return this.union;
    }
    
    public RelationalAlgebraOperation getUnionAll() {
        return this.unionAll;
    }
    
    public SortSpecification getSort() {
        return this.sort;
    }
    
    public JoinSupport getJoin() {
        return this.join;
    }
}
