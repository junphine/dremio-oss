package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.legacy.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.rel.type.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import java.util.*;

public class PostgresRelToSqlConverter extends JdbcDremioRelToSqlConverter
{
    public PostgresRelToSqlConverter(final JdbcDremioSqlDialect dialect) {
        super(dialect);
    }
    
    @Override
    protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
        return this;
    }
    
    protected void generateGroupBy(final DremioRelToSqlConverter.Builder builder, final Aggregate e) {
        final List<SqlNode> groupByList = (List<SqlNode>)Expressions.list();
        final List<SqlNode> selectList = new ArrayList<SqlNode>();
        for (final int group : e.getGroupSet()) {
            final SqlNode field = builder.context.field(group);
            this.addSelect((List)selectList, field, e.getRowType());
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
            if (isDecimal(aggCall.getType())) {
                aggCallSqlNode = (SqlNode)SqlStdOperatorTable.CAST.createCall(PostgresRelToSqlConverter.POS, new SqlNode[] { aggCallSqlNode, this.getDialect().getCastSpec(aggCall.getType()) });
            }
            this.addSelect((List)selectList, aggCallSqlNode, e.getRowType());
        }
        builder.setSelect(new SqlNodeList((Collection)selectList, PostgresRelToSqlConverter.POS));
        if (groupByList.isEmpty()) {
            if (e.getAggCallList().isEmpty()) {
                for (final int group : e.getGroupSet()) {
                    groupByList.add((SqlNode)SqlLiteral.createExactNumeric(String.valueOf(group + 1), SqlParserPos.ZERO));
                }
                if (groupByList.isEmpty()) {
                    builder.setGroupBy((SqlNodeList)null);
                }
                else {
                    builder.setGroupBy(new SqlNodeList((Collection)groupByList, PostgresRelToSqlConverter.POS));
                }
            }
        }
        else {
            builder.setGroupBy(new SqlNodeList((Collection)groupByList, PostgresRelToSqlConverter.POS));
        }
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
                        case "bpchar":
                        case "char":
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
    
    @Override
    public SqlImplementor.Context aliasContext(final Map<String, RelDataType> aliases, final boolean qualified) {
        return (SqlImplementor.Context)new PostgreSQLAliasContext(aliases, qualified);
    }
    
    class PostgreSQLAliasContext extends DremioRelToSqlConverter.DremioAliasContext
    {
        public PostgreSQLAliasContext(final Map<String, RelDataType> aliases, final boolean qualified) {
            super((DremioRelToSqlConverter)PostgresRelToSqlConverter.this, (Map)aliases, qualified);
        }
        
        public SqlNode toSql(final RexProgram program, final RexNode rex) {
            final SqlNode sqlNode = super.toSql(program, rex);
            if (rex.getKind().equals((Object)SqlKind.LITERAL) && rex.getType().getSqlTypeName().equals((Object)SqlTypeName.DOUBLE)) {
                return this.checkAndAddFloatCast(rex, sqlNode);
            }
            return sqlNode;
        }
        
        private SqlNode checkAndAddFloatCast(final RexNode rex, final SqlNode sqlCall) {
            final SqlIdentifier typeIdentifier = new SqlIdentifier(SqlTypeName.FLOAT.name(), SqlParserPos.ZERO);
            final SqlDataTypeSpec spec = new SqlDataTypeSpec(typeIdentifier, -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            return (SqlNode)SqlStdOperatorTable.CAST.createCall(SqlImplementor.POS, new SqlNode[] { sqlCall, spec });
        }
    }
}
