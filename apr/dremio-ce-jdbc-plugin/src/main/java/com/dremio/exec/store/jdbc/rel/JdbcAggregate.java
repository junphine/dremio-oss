package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.planner.common.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.util.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.store.jdbc.conf.*;
import com.dremio.common.dialect.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.sql.validate.*;
import java.util.*;
import com.google.common.collect.*;
import com.dremio.exec.store.jdbc.legacy.*;

public class JdbcAggregate extends Aggregate implements JdbcRelImpl
{
    private final StoragePluginId pluginId;
    
    public JdbcAggregate(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode input, final boolean indicator, final ImmutableBitSet groupSet, final List<ImmutableBitSet> groupSets, final List<AggregateCall> aggCalls, final StoragePluginId pluginId) throws InvalidRelException {
        super(cluster, traitSet, input, indicator, groupSet, (List)groupSets, (List)aggCalls);
        assert this.groupSets.size() == 1 : "Grouping sets not supported";
        assert !this.indicator;
        DremioSqlDialect dialect;
        if (null != (this.pluginId = pluginId) && pluginId.getConnectionConf() instanceof DialectConf) {
            final DialectConf<?, ?> conf = (DialectConf<?, ?>)pluginId.getConnectionConf();
            dialect = conf.getDialect();
        }
        else {
            dialect = DremioSqlDialect.CALCITE;
        }
        for (final AggregateCall aggCall : aggCalls) {
            if (!dialect.supportsAggregateFunction(aggCall.getAggregation().getKind())) {
                throw new InvalidRelException("cannot implement aggregate function " + aggCall.getAggregation());
            }
        }
    }
    
    public JdbcAggregate copy(final RelTraitSet traitSet, final RelNode input, final boolean indicator, final ImmutableBitSet groupSet, final List<ImmutableBitSet> groupSets, final List<AggregateCall> aggCalls) {
        try {
            return new JdbcAggregate(this.getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls, this.pluginId);
        }
        catch (InvalidRelException e) {
            throw new AssertionError(e);
        }
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode input = this.getInput().accept((RelShuttle)copier);
        try {
            return (RelNode)new JdbcAggregate(copier.getCluster(), this.getTraitSet(), input, this.indicator, this.getGroupSet(), (List<ImmutableBitSet>)this.getGroupSets(), copier.copyOf(this.getAggCallList()), this.pluginId);
        }
        catch (InvalidRelException e) {
            throw new AssertionError(e);
        }
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public RelNode revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return builder.push((RelNode)revertedInputs.get(0)).aggregate(builder.groupKey(this.groupSet, this.indicator, this.groupSets), this.aggCalls).build();
    }
    
    public RelNode shortenAliases(final SqlValidatorUtil.Suggester suggester, final Set<String> usedAliases) {
        if (this.pluginId == null) {
            return (RelNode)this;
        }
        final DialectConf<?, ?> conf = (DialectConf<?, ?>)this.pluginId.getConnectionConf();
        final JdbcDremioSqlDialect dialect = conf.getDialect();
        if (dialect.getIdentifierLengthLimit() == null) {
            return (RelNode)this;
        }
        boolean needsShortenedAlias = false;
        final ImmutableList.Builder<AggregateCall> aggCallBuilder = ImmutableList.builder();
        for (final AggregateCall aggCall : this.aggCalls) {
            if (aggCall.getName() != null && aggCall.getName().length() > dialect.getIdentifierLengthLimit()) {
                needsShortenedAlias = true;
                final String newAlias = SqlValidatorUtil.uniquify((String)null, (Set)usedAliases, suggester);
                aggCallBuilder.add(aggCall.rename(newAlias));
            }
            else {
                aggCallBuilder.add(aggCall);
            }
        }
        if (!needsShortenedAlias) {
            return (RelNode)this;
        }
        return (RelNode)this.copy(this.getTraitSet(), this.input, this.indicator, this.getGroupSet(), (List<ImmutableBitSet>)this.getGroupSets(), (List<AggregateCall>)aggCallBuilder.build());
    }
}
