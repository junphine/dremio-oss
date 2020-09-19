package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.sql.fun.*;
import com.dremio.exec.store.jdbc.dialect.*;
import org.apache.calcite.rel.type.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.google.common.collect.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.*;
import java.util.*;

public class OracleRelToSqlConverter extends JdbcDremioRelToSqlConverter
{
    private static final String ORACLE_OFFSET_ALIAS = "ORA_RNUMOFFSET$";
    private boolean canPushOrderByOut;
    
    public OracleRelToSqlConverter(final JdbcDremioSqlDialect dialect) {
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
        try {
            this.canPushOrderByOut = false;
            DremioRelToSqlConverter.Builder builder = null;
            final RexBuilder rexBuilder = e.getCluster().getRexBuilder();
            if (e.fetch != null) {
                RexNode limitRex;
                if (e.offset != null) {
                    builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.SELECT, SqlImplementor.Clause.WHERE, SqlImplementor.Clause.FETCH });
                    limitRex = rexBuilder.makeCall((SqlOperator)SqlStdOperatorTable.PLUS, new RexNode[] { e.fetch, e.offset });
                }
                else {
                    builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.WHERE, SqlImplementor.Clause.FETCH });
                    limitRex = e.fetch;
                }
                final RexNode rowNumLimit = rexBuilder.makeCall((SqlOperator)SqlStdOperatorTable.LESS_THAN_OR_EQUAL, new RexNode[] { rexBuilder.makeFlag((Enum)OracleDialect.OracleKeyWords.ROWNUM), limitRex });
                builder.setWhere(builder.context.toSql((RexProgram)null, rowNumLimit));
                if (e.offset == null) {
                    x = builder.result();
                }
            }
            if (e.offset != null) {
                if (e.fetch == null) {
                    builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.SELECT });
                }
                final List<SqlNode> selectList = new ArrayList<SqlNode>();
                final RexNode offsetRowNumRex = (RexNode)rexBuilder.makeFlag((Enum)OracleDialect.OracleKeyWords.ROWNUM);
                SqlNode offsetRowNum = builder.context.toSql((RexProgram)null, offsetRowNumRex);
                offsetRowNum = (SqlNode)SqlStdOperatorTable.AS.createCall(OracleRelToSqlConverter.POS, new SqlNode[] { offsetRowNum, new SqlIdentifier("ORA_RNUMOFFSET$", OracleRelToSqlConverter.POS) });
                selectList.add(offsetRowNum);
                selectList.addAll((x.asSelect().getSelectList() == null) ? x.qualifiedContext().fieldList() : x.asSelect().getSelectList().getList());
                final Map<String, RelDataType> aliases = this.getAliases(builder);
                final List<RelDataTypeField> fields = Lists.newArrayList();
                fields.add((RelDataTypeField)new RelDataTypeFieldImpl("ORA_RNUMOFFSET$", 0, offsetRowNumRex.getType()));
                fields.addAll(aliases.values().iterator().next().getFieldList());
                final RelRecordType recordType = new RelRecordType((List)fields);
                final List<RexNode> rexNodes = Lists.newArrayList();
                rexNodes.add(offsetRowNumRex);
                rexNodes.addAll(rexBuilder.identityProjects((RelDataType)recordType).subList(1, fields.size()));
                final JdbcProject project = new JdbcProject(e.getCluster(), e.getTraitSet(), (RelNode)e, rexNodes, (RelDataType)recordType, ((JdbcSort)e).getPluginId());
                aliases.put(aliases.keySet().iterator().next(), (RelDataType)recordType);
                if (e.fetch == null && e.getCollation().getFieldCollations().isEmpty()) {
                    builder.setSelect(new SqlNodeList((Collection)selectList, SqlImplementor.POS));
                }
                else {
                    builder.setSelect(new SqlNodeList((Collection)this.getIdentifiers(selectList, 1), SqlImplementor.POS));
                }
                final List<SqlImplementor.Clause> clauses = (List<SqlImplementor.Clause>)ImmutableList.of(SqlImplementor.Clause.SELECT);
                x = this.result((SqlNode)builder.result().asSelect(), (Collection)clauses, (RelNode)project, (Map)aliases);
                builder = x.builder((RelNode)e, new SqlImplementor.Clause[] { SqlImplementor.Clause.SELECT, SqlImplementor.Clause.WHERE });
                builder.setSelect(new SqlNodeList((Collection)this.getIdentifiers(selectList.subList(1, selectList.size()), 0), SqlImplementor.POS));
                final RexNode rowNumOffsetClause = rexBuilder.makeCall((SqlOperator)SqlStdOperatorTable.GREATER_THAN, new RexNode[] { rexBuilder.makeInputRef((RelNode)project, 0), e.offset });
                builder.setWhere(x.qualifiedContext().toSql((RexProgram)null, rowNumOffsetClause));
                x = builder.result();
            }
        }
        finally {
            this.canPushOrderByOut = true;
        }
        return x;
    }
    
    private Collection<? extends SqlNode> getIdentifiers(final List<SqlNode> nodeList, final int startTransformIndex) {
        final ImmutableList.Builder<SqlNode> nodeListBuilder = (ImmutableList.Builder<SqlNode>)new ImmutableList.Builder();
        for (int i = 0; i < nodeList.size(); ++i) {
            final SqlNode node = nodeList.get(i);
            if (i >= startTransformIndex && node.getKind() == SqlKind.AS) {
                nodeListBuilder.add(((SqlCall)node).operand(1));
            }
            else if (i >= startTransformIndex && node.getKind() == SqlKind.IDENTIFIER) {
                final SqlIdentifier identNode = (SqlIdentifier)node;
                nodeListBuilder.add(new SqlIdentifier((String)identNode.names.get(identNode.names.size() - 1), identNode.getParserPosition()));
            }
            else {
                nodeListBuilder.add(node);
            }
        }
        return (Collection<? extends SqlNode>)nodeListBuilder.build();
    }
    
    @Override
    public SqlImplementor.Context aliasContext(final Map<String, RelDataType> aliases, final boolean qualified) {
        return (SqlImplementor.Context)new OracleAliasContext(aliases, qualified);
    }
    
    private Map<String, RelDataType> getAliases(final DremioRelToSqlConverter.Builder builder) {
        if (builder.context instanceof DremioRelToSqlConverter.DremioAliasContext) {
            return new HashMap<String, RelDataType>(((DremioRelToSqlConverter.DremioAliasContext)builder.context).getAliases());
        }
        final Map<String, RelDataType> aliases = Maps.newHashMap();
        aliases.put(builder.getAliases().entrySet().iterator().next().getKey(), builder.getNeedType());
        return aliases;
    }
    
    public SqlWindow adjustWindowForSource(final DremioRelToSqlConverter.DremioContext context, final SqlAggFunction op, final SqlWindow window) {
        final List<SqlAggFunction> opsToAddOrderByTo = (List<SqlAggFunction>)ImmutableList.of(SqlStdOperatorTable.ROW_NUMBER, SqlStdOperatorTable.LAG, SqlStdOperatorTable.LEAD, SqlStdOperatorTable.NTILE);
        return addDummyOrderBy(window, context, op, (List)opsToAddOrderByTo);
    }
    
    class OracleAliasContext extends DremioRelToSqlConverter.DremioAliasContext
    {
        public OracleAliasContext(final Map<String, RelDataType> aliases, final boolean qualified) {
            super(aliases, qualified);
        }
        
        public SqlCall toSql(final RexProgram program, final RexOver rexOver) {
            final SqlCall sqlCall = super.toSql(program, rexOver);
            if (rexOver.getType().getSqlTypeName().equals(SqlTypeName.DOUBLE)) {
                return (SqlCall)this.checkAndAddFloatCast((RexNode)rexOver, (SqlNode)sqlCall);
            }
            return sqlCall;
        }
        
        public SqlNode toSql(final RexProgram program, final RexNode rex) {
            final SqlNode sqlNode = super.toSql(program, rex);
            if (rex.getKind().equals(SqlKind.LITERAL) && rex.getType().getSqlTypeName().equals(SqlTypeName.DOUBLE)) {
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
