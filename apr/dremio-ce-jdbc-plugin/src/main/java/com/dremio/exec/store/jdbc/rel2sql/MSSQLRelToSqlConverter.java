package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.legacy.*;
import com.google.common.collect.*;
import org.apache.calcite.rel.type.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.*;
import com.google.common.base.*;
import java.util.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;

public class MSSQLRelToSqlConverter extends JdbcDremioRelToSqlConverter
{
    private static final SqlFunction CONVERT_FUNCTION;
    private static final SqlNode MSSQL_ODBC_FORMAT_SPEC;
    
    public MSSQLRelToSqlConverter(final JdbcDremioSqlDialect dialect) {
        super(dialect);
    }
    
    @Override
    protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
        return this;
    }
    
    public void addSelect(final List<SqlNode> selectList, final SqlNode node, final RelDataType rowType) {
        if (node instanceof SqlIdentifier && ((SqlIdentifier)node).getCollation() != null) {
            final String name = rowType.getFieldNames().get(selectList.size());
            selectList.add(SqlStdOperatorTable.AS.createCall(MSSQLRelToSqlConverter.POS, new SqlNode[] { node, new SqlIdentifier(name, MSSQLRelToSqlConverter.POS) }));
        }
        else {
            super.addSelect(selectList, node, rowType);
        }
    }
    
    public SqlWindow adjustWindowForSource(final DremioRelToSqlConverter.DremioContext context, final SqlAggFunction op, final SqlWindow window) {
        final List<SqlAggFunction> opsToAddOrderByTo = ImmutableList.of(SqlStdOperatorTable.ROW_NUMBER, SqlStdOperatorTable.CUME_DIST, SqlStdOperatorTable.LAG, SqlStdOperatorTable.LEAD, SqlStdOperatorTable.NTILE, SqlStdOperatorTable.LAST_VALUE, SqlStdOperatorTable.FIRST_VALUE);
        return addDummyOrderBy(window, context, op, opsToAddOrderByTo);
    }
    
    protected boolean canAddCollation(final RelDataTypeField field) {
        if (field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
            final String lowerCaseName = field.getName().toLowerCase(Locale.ROOT);
            final Map<String, String> properties = this.columnProperties.get(lowerCaseName);
            if (properties != null) {
                final String typeName = properties.get("sourceTypeName");
                if (typeName != null) {
                    final String s = typeName;
                    switch (s) {
                        case "char":
                        case "nchar":
                        case "ntext":
                        case "nvarchar":
                        case "text":
                        case "varchar": {
                            return true;
                        }
                        default: {
                            return false;
                        }
                    }
                }
            }
        }
        return super.canAddCollation(field);
    }
    
    public DremioRelToSqlConverter.Result visit(final Sort e) {
        DremioRelToSqlConverter.Result x = (DremioRelToSqlConverter.Result)this.visitChild(0, e.getInput());
        final DremioRelToSqlConverter.Result orderByResult = this.visitOrderByHelper(x, e);
        final SqlNodeList orderByList = orderByResult.asSelect().getOrderList();
        final boolean hasOffset = !JdbcSort.isOffsetEmpty(e);
        final boolean hasFetch = e.fetch != null;
        final boolean hadOrderByListOfLiterals = orderByList == null || orderByList.getList().isEmpty();
        if (hadOrderByListOfLiterals && !hasOffset && !hasFetch) {
            return x;
        }
        if (hasOffset && hasFetch && (long)((RexLiteral)e.fetch).getValue2() == 0L) {
            final DremioRelToSqlConverter.Builder builder = x.builder((RelNode)e, true, new SqlImplementor.Clause[] { SqlImplementor.Clause.SELECT, SqlImplementor.Clause.FETCH, SqlImplementor.Clause.ORDER_BY });
            builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
            final List<SqlNode> newOrderByList = Expressions.list();
            for (final RelFieldCollation field : e.getCollation().getFieldCollations()) {
                builder.addOrderItem(newOrderByList, field);
            }
            if (!newOrderByList.isEmpty()) {
                builder.setOrderBy(new SqlNodeList(newOrderByList, MSSQLRelToSqlConverter.POS));
            }
            return builder.result();
        }
        if ((hadOrderByListOfLiterals || JdbcSort.isCollationEmpty(e)) && !hasOffset) {
            Preconditions.checkState(hasFetch);
            final DremioRelToSqlConverter.Builder builder = x.builder((RelNode)e, true, new SqlImplementor.Clause[] { SqlImplementor.Clause.SELECT, SqlImplementor.Clause.FETCH });
            builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
            return builder.result();
        }
        x = this.visitFetchAndOffsetHelper(orderByResult, e);
        return x;
    }
    
    protected void generateGroupBy(final DremioRelToSqlConverter.Builder builder, final Aggregate e) {
        final List<SqlNode> groupByList = Expressions.list();
        final List<SqlNode> selectList = new ArrayList<SqlNode>();
        for (final int group : e.getGroupSet()) {
            final SqlNode field = builder.context.field(group);
            this.addSelect(selectList, field, e.getRowType());
            if (field.getKind() != SqlKind.LITERAL || ((SqlLiteral)field).getTypeName() != SqlTypeName.NULL) {
                groupByList.add(field);
            }
        }
        for (final AggregateCall aggCall : e.getAggCallList()) {
            SqlNode aggCallSqlNode = ((DremioRelToSqlConverter.DremioContext)builder.context).toSql(aggCall, e);
            if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
                aggCallSqlNode = this.dialect.rewriteSingleValueExpr(aggCallSqlNode);
            }
            this.addSelect(selectList, aggCallSqlNode, e.getRowType());
        }
        builder.setSelect(new SqlNodeList(selectList, MSSQLRelToSqlConverter.POS));
        if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
            builder.setGroupBy(new SqlNodeList(groupByList, MSSQLRelToSqlConverter.POS));
        }
    }
    
    public SqlNode convertCallToSql(final DremioRelToSqlConverter.DremioContext context, final RexProgram program, final RexCall call, final boolean ignoreCast) {
        if (ignoreCast || call.getKind() != SqlKind.CAST) {
            return null;
        }
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        final SqlTypeName sourceType = operands.get(0).getType().getSqlTypeName();
        final SqlTypeName targetType = call.getType().getSqlTypeName();
        if (SqlTypeName.DATETIME_TYPES.contains(sourceType) && SqlTypeName.CHAR_TYPES.contains(targetType)) {
            final SqlNode sourceTypeNode = context.toSql(program, (RexNode)operands.get(0));
            final SqlNode targetTypeNode = this.dialect.getCastSpec(call.getType());
            final SqlNodeList nodeList = new SqlNodeList(ImmutableList.of(targetTypeNode, sourceTypeNode, MSSQLRelToSqlConverter.MSSQL_ODBC_FORMAT_SPEC), SqlParserPos.ZERO);
            return (SqlNode)MSSQLRelToSqlConverter.CONVERT_FUNCTION.createCall(nodeList);
        }
        return null;
    }
    
    static {
        CONVERT_FUNCTION = new SqlFunction("CONVERT", SqlKind.OTHER_FUNCTION, (SqlReturnTypeInference)null, InferTypes.FIRST_KNOWN, (SqlOperandTypeChecker)null, SqlFunctionCategory.SYSTEM);
        MSSQL_ODBC_FORMAT_SPEC = (SqlNode)SqlLiteral.createExactNumeric("121", SqlParserPos.ZERO);
    }
}
