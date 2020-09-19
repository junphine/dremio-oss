package com.dremio.exec.store.jdbc.rel2sql;

import org.apache.commons.lang3.tuple.*;
import com.dremio.exec.store.jdbc.legacy.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.*;
import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.*;
import java.util.*;
import com.google.common.collect.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rel.core.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.store.jdbc.rel.*;

public class MySQLRelToSqlConverter extends JdbcDremioRelToSqlConverter
{
    private final ArrayDeque<MutablePair<Project, Boolean>> projectionContext;
    
    public MySQLRelToSqlConverter(final JdbcDremioSqlDialect dialect) {
        super(dialect);
        this.projectionContext = new ArrayDeque<MutablePair<Project, Boolean>>();
    }
    
    @Override
    protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
        return this;
    }
    
    public DremioRelToSqlConverter.Result visit(final Project project) {
        final MutablePair<Project, Boolean> pair = (MutablePair<Project, Boolean>)new MutablePair(project, Boolean.FALSE);
        this.projectionContext.addLast(pair);
        DremioRelToSqlConverter.Result res = (DremioRelToSqlConverter.Result)this.visitChild(0, project.getInput());
        if (!(boolean)pair.right) {
            res = this.processProjectChild(project, res);
        }
        this.projectionContext.removeLast();
        return res;
    }
    
    public DremioRelToSqlConverter.Result visit(final Join e) {
        if (e.getJoinType() != JoinRelType.FULL) {
            return (DremioRelToSqlConverter.Result)super.visit(e);
        }
        final Join leftJoin = duplicateJoinAndChangeType(e, JoinRelType.LEFT);
        final Join rightJoin = duplicateJoinAndChangeType(e, JoinRelType.RIGHT);
        final RexBuilder builder = e.getCluster().getRexBuilder();
        final RexNode whereCondition = builder.makeCall((SqlOperator)SqlStdOperatorTable.IS_NULL, new RexNode[] { builder.makeInputRef(e.getLeft(), 0) });
        final StoragePluginId pluginId = (e instanceof JdbcRelImpl) ? ((JdbcRelImpl)e).getPluginId() : null;
        final JdbcFilter filter = new JdbcFilter(e.getCluster(), e.getTraitSet(), (RelNode)rightJoin, whereCondition, ImmutableSet.of(), pluginId);
        RelNode leftUnionOp;
        RelNode rightUnionOp;
        if (this.projectionContext.isEmpty()) {
            leftUnionOp = (RelNode)leftJoin;
            rightUnionOp = (RelNode)filter;
        }
        else {
            final MutablePair<Project, Boolean> originalProjectPair = this.projectionContext.getLast();
            final Project originalProject = (Project)originalProjectPair.left;
            leftUnionOp = originalProject.copy(originalProject.getTraitSet(), (List)Collections.singletonList(leftJoin));
            rightUnionOp = originalProject.copy(originalProject.getTraitSet(), (List)Collections.singletonList(filter));
            originalProjectPair.right = Boolean.TRUE;
        }
        final List<RelNode> nodes = (List<RelNode>)Lists.newArrayList(new RelNode[] { leftUnionOp, rightUnionOp });
        final JdbcUnion union = new JdbcUnion(e.getCluster(), e.getTraitSet(), nodes, true, pluginId);
        return (DremioRelToSqlConverter.Result)super.visit((Union)union);
    }
    
    protected DremioRelToSqlConverter.Result visitFetchAndOffsetHelper(DremioRelToSqlConverter.Result x, final Sort e) {
        if (e.fetch != null) {
            final DremioRelToSqlConverter.Builder builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.FETCH });
            builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
            x = builder.result();
        }
        if (!JdbcSort.isOffsetEmpty(e)) {
            final DremioRelToSqlConverter.Builder builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.OFFSET });
            builder.setOffset(builder.context.toSql((RexProgram)null, e.offset));
            x = builder.result();
        }
        return x;
    }
    
    private static Join duplicateJoinAndChangeType(final Join e, final JoinRelType type) {
        return e.copy(e.getTraitSet(), e.getCondition(), e.getLeft(), e.getRight(), type, e.isSemiJoinDone());
    }
}
