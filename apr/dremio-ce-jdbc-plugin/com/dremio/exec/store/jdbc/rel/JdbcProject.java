package com.dremio.exec.store.jdbc.rel;

import org.apache.calcite.rel.core.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.sql.validate.*;
import java.util.*;
import com.dremio.exec.store.jdbc.conf.*;
import com.dremio.exec.store.jdbc.legacy.*;
import org.apache.calcite.rel.type.*;

public class JdbcProject extends Project implements JdbcRelImpl
{
    private final boolean foundContains;
    private final StoragePluginId pluginId;
    private final boolean isDummy;
    
    public JdbcProject(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode input, final List<? extends RexNode> projects, final RelDataType rowType, final StoragePluginId pluginId) {
        this(cluster, traitSet, input, projects, rowType, pluginId, false);
    }
    
    JdbcProject(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode input, final List<? extends RexNode> projects, final RelDataType rowType, final StoragePluginId pluginId, final boolean isDummy) {
        super(cluster, traitSet, input, (List)projects, rowType);
        this.pluginId = pluginId;
        boolean foundContains = false;
        for (final RexNode rex : this.getChildExps()) {
            if (MoreRelOptUtil.ContainsRexVisitor.hasContains(rex)) {
                foundContains = true;
                break;
            }
        }
        this.foundContains = foundContains;
        this.isDummy = isDummy;
    }
    
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        if (this.foundContains) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        final double rowCount = mq.getRowCount((RelNode)this);
        return planner.getCostFactory().makeCost(rowCount, 0.0, 0.0);
    }
    
    public JdbcProject copy(final RelTraitSet traitSet, final RelNode input, final List<RexNode> projects, final RelDataType rowType) {
        return new JdbcProject(this.getCluster(), traitSet, input, projects, rowType, this.pluginId);
    }
    
    public RelNode copyWith(final CopyWithCluster copier) {
        final RelNode input = this.getInput().accept((RelShuttle)copier);
        return (RelNode)new JdbcProject(copier.getCluster(), this.getTraitSet(), input, copier.copyRexNodes(this.getProjects()), copier.copyOf(this.getRowType()), this.pluginId);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public Project revert(final List<RelNode> revertedInputs, final RelBuilder builder) {
        return (Project)builder.push((RelNode)revertedInputs.get(0)).projectNamed((Iterable)this.getProjects(), (Iterable)this.getRowType().getFieldNames(), true).build();
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
        final RelDataType types = this.getRowType();
        boolean needsShortenedAlias = false;
        final RelDataTypeFactory.Builder shortenedFieldBuilder = (RelDataTypeFactory.Builder)this.getCluster().getTypeFactory().builder();
        for (final RelDataTypeField originalField : types.getFieldList()) {
            if (originalField.getName().length() > dialect.getIdentifierLengthLimit()) {
                needsShortenedAlias = true;
                final String newAlias = SqlValidatorUtil.uniquify((String)null, (Set)usedAliases, suggester);
                shortenedFieldBuilder.add(newAlias, originalField.getType());
            }
            else {
                shortenedFieldBuilder.add(originalField);
            }
        }
        if (!needsShortenedAlias) {
            return (RelNode)this;
        }
        return (RelNode)this.copy(this.traitSet, this.input, (List<RexNode>)this.exps, shortenedFieldBuilder.build());
    }
    
    public boolean isDummyProject() {
        return this.isDummy;
    }
}
