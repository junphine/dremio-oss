package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.legacy.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.validate.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import java.util.*;

public class RedshiftRelToSqlConverter extends JdbcDremioRelToSqlConverter
{
    public RedshiftRelToSqlConverter(final JdbcDremioSqlDialect dialect) {
        super(dialect);
    }
    
    @Override
    protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
        return this;
    }
    
    public void addSelect(final List<SqlNode> selectList, SqlNode node, final RelDataType rowType) {
        final String name = rowType.getFieldNames().get(selectList.size());
        final String alias = SqlValidatorUtil.getAlias(node, -1);
        if (alias == null || !alias.equals(name)) {
            if (name.equals("*")) {
                node = (SqlNode)SqlStdOperatorTable.AS.createCall(RedshiftRelToSqlConverter.POS, new SqlNode[] { node, new SqlIdentifier(SqlUtil.deriveAliasFromOrdinal(selectList.size()), RedshiftRelToSqlConverter.POS) });
            }
            else {
                node = (SqlNode)SqlStdOperatorTable.AS.createCall(RedshiftRelToSqlConverter.POS, new SqlNode[] { node, new SqlIdentifier(name, RedshiftRelToSqlConverter.POS) });
            }
        }
        selectList.add(node);
    }
    
    protected void generateGroupBy(final DremioRelToSqlConverter.Builder builder, final Aggregate e) {
        final List<SqlNode> groupByList = Expressions.list();
        final List<SqlNode> selectList = new ArrayList<SqlNode>();
        for (final int group : e.getGroupSet()) {
            final SqlNode field = builder.context.field(group);
            this.addSelect(selectList, field, e.getRowType());
            if (field.getKind() != SqlKind.LITERAL || ((SqlLiteral)field).getTypeName() != SqlTypeName.NULL) {
                if (field.getKind() == SqlKind.IDENTIFIER && ((SqlIdentifier)field).getCollation() != null) {
                    groupByList.add((SqlNode)new SqlIdentifier((List)((SqlIdentifier)field).names, (SqlCollation)null, field.getParserPosition(), (List)null));
                }
                else {
                    groupByList.add(field);
                }
            }
        }
        for (final AggregateCall aggCall : e.getAggCallList()) {
            SqlNode aggCallSqlNode = ((DremioRelToSqlConverter.DremioContext)builder.context).toSql(aggCall, e);
            if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
                aggCallSqlNode = this.dialect.rewriteSingleValueExpr(aggCallSqlNode);
            }
            this.addSelect(selectList, aggCallSqlNode, e.getRowType());
        }
        builder.setSelect(new SqlNodeList((Collection)selectList, RedshiftRelToSqlConverter.POS));
        if (groupByList.isEmpty()) {
            if (e.getAggCallList().isEmpty()) {
                for (final int group : e.getGroupSet()) {
                    groupByList.add((SqlNode)SqlLiteral.createExactNumeric(String.valueOf(group + 1), SqlParserPos.ZERO));
                }
                builder.setGroupBy(new SqlNodeList((Collection)groupByList, RedshiftRelToSqlConverter.POS));
            }
        }
        else {
            builder.setGroupBy(new SqlNodeList((Collection)groupByList, RedshiftRelToSqlConverter.POS));
        }
    }
}
