package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.rel2sql.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.fun.*;
import com.google.common.collect.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.google.common.base.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.parser.*;
import java.util.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.*;

public class MSSQLLegacyRelToSqlConverter extends JdbcDremioRelToSqlConverter
{
    private static final SqlFunction CONVERT_FUNCTION;
    private static final SqlNode MSSQL_ODBC_FORMAT_SPEC;
    
    public MSSQLLegacyRelToSqlConverter(final JdbcDremioSqlDialect dialect) {
        super(dialect);
    }
    
    @Override
    protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
        return this;
    }
    
    public void addSelect(final List<SqlNode> selectList, final SqlNode node, final RelDataType rowType) {
        if (node instanceof SqlIdentifier && ((SqlIdentifier)node).getCollation() != null) {
            final String name = rowType.getFieldNames().get(selectList.size());
            selectList.add((SqlNode)SqlStdOperatorTable.AS.createCall(MSSQLLegacyRelToSqlConverter.POS, new SqlNode[] { node, new SqlIdentifier(name, MSSQLLegacyRelToSqlConverter.POS) }));
        }
        else {
            super.addSelect((List)selectList, node, rowType);
        }
    }
    
    public SqlWindow adjustWindowForSource(final DremioRelToSqlConverter.DremioContext context, final SqlAggFunction op, final SqlWindow window) {
        final List<SqlAggFunction> opsToAddOrderByTo = (List<SqlAggFunction>)ImmutableList.of((Object)SqlStdOperatorTable.ROW_NUMBER, (Object)SqlStdOperatorTable.CUME_DIST, (Object)SqlStdOperatorTable.LAG, (Object)SqlStdOperatorTable.LEAD, (Object)SqlStdOperatorTable.NTILE, (Object)SqlStdOperatorTable.LAST_VALUE, (Object)SqlStdOperatorTable.FIRST_VALUE);
        return addDummyOrderBy(window, context, op, (List)opsToAddOrderByTo);
    }
    
    public DremioRelToSqlConverter.Result visit(final Sort e) {
        DremioRelToSqlConverter.Result x = (DremioRelToSqlConverter.Result)this.visitChild(0, e.getInput());
        final DremioRelToSqlConverter.Result orderByResult = this.visitOrderByHelper(x, e);
        final SqlNodeList orderByList = orderByResult.asSelect().getOrderList();
        final boolean hadOrderByListOfLiterals = orderByList == null || orderByList.getList().isEmpty();
        if (hadOrderByListOfLiterals && JdbcSort.isOffsetEmpty(e) && e.fetch == null) {
            return x;
        }
        if ((hadOrderByListOfLiterals || JdbcSort.isCollationEmpty(e)) && JdbcSort.isOffsetEmpty(e)) {
            Preconditions.checkState(e.fetch != null);
            final DremioRelToSqlConverter.Builder builder = x.builder((RelNode)e, true, new SqlImplementor.Clause[] { SqlImplementor.Clause.SELECT, SqlImplementor.Clause.FETCH });
            builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
            return builder.result();
        }
        x = this.visitFetchAndOffsetHelper(orderByResult, e);
        return x;
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
            final SqlNodeList nodeList = new SqlNodeList((Collection)ImmutableList.of((Object)targetTypeNode, (Object)sourceTypeNode, (Object)MSSQLLegacyRelToSqlConverter.MSSQL_ODBC_FORMAT_SPEC), SqlParserPos.ZERO);
            return (SqlNode)MSSQLLegacyRelToSqlConverter.CONVERT_FUNCTION.createCall(nodeList);
        }
        return null;
    }
    
    static {
        CONVERT_FUNCTION = new SqlFunction("CONVERT", SqlKind.OTHER_FUNCTION, (SqlReturnTypeInference)null, InferTypes.FIRST_KNOWN, (SqlOperandTypeChecker)null, SqlFunctionCategory.SYSTEM);
        MSSQL_ODBC_FORMAT_SPEC = (SqlNode)SqlLiteral.createExactNumeric("121", SqlParserPos.ZERO);
    }
}
