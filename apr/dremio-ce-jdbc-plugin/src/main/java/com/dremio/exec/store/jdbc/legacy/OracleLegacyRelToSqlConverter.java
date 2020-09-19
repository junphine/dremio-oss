package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import com.google.common.collect.*;
import java.util.*;

public class OracleLegacyRelToSqlConverter extends JdbcDremioRelToSqlConverter
{
    private boolean canPushOrderByOut;
    
    public OracleLegacyRelToSqlConverter(final JdbcDremioSqlDialect dialect) {
        super(dialect);
        this.canPushOrderByOut = true;
    }
    
    @Override
    protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
        return this;
    }
    
    public boolean shouldPushOrderByOut() {
        return this.canPushOrderByOut;
    }
    
    public DremioRelToSqlConverter.Result visit(final Filter e) {
        final DremioRelToSqlConverter.Result x = (DremioRelToSqlConverter.Result)this.visitChild(0, e.getInput());
        final DremioRelToSqlConverter.Builder builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.WHERE });
        builder.setWhere(builder.context.toSql((RexProgram)null, e.getCondition()));
        return builder.result();
    }
    
    public DremioRelToSqlConverter.Result visit(final Sort e) {
        DremioRelToSqlConverter.Result x = (DremioRelToSqlConverter.Result)this.visitChild(0, e.getInput());
        x = this.visitOrderByHelper(x, e);
        if (e.fetch != null) {
            try {
                this.canPushOrderByOut = false;
                final DremioRelToSqlConverter.Builder builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.WHERE, SqlImplementor.Clause.FETCH });
                final RexBuilder rexBuilder = e.getCluster().getRexBuilder();
                final RexNode rowNumLimit = rexBuilder.makeCall((SqlOperator)SqlStdOperatorTable.LESS_THAN_OR_EQUAL, new RexNode[] { rexBuilder.makeFlag((Enum)OracleLegacyDialect.OracleKeyWords.ROWNUM), e.fetch });
                builder.setWhere(builder.context.toSql((RexProgram)null, rowNumLimit));
                x = builder.result();
            }
            finally {
                this.canPushOrderByOut = true;
            }
        }
        return x;
    }
    
    public SqlWindow adjustWindowForSource(final DremioRelToSqlConverter.DremioContext context, final SqlAggFunction op, final SqlWindow window) {
        final List<SqlAggFunction> opsToAddOrderByTo = (List<SqlAggFunction>)ImmutableList.of(SqlStdOperatorTable.ROW_NUMBER, SqlStdOperatorTable.LAG, SqlStdOperatorTable.LEAD, SqlStdOperatorTable.NTILE);
        return addDummyOrderBy(window, context, op, (List)opsToAddOrderByTo);
    }
}
