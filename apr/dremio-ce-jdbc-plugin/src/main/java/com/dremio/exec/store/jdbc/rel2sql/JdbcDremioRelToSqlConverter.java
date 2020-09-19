package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.common.dialect.*;
import org.apache.calcite.rel.core.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.util.*;
import java.util.*;
import com.dremio.exec.store.jdbc.rel.*;
import java.math.*;
import com.google.common.collect.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.*;

public abstract class JdbcDremioRelToSqlConverter extends DremioRelToSqlConverter
{
    protected Map<String, Map<String, String>> columnProperties;
    
    public JdbcDremioRelToSqlConverter(final JdbcDremioSqlDialect dremioDialect) {
        super((DremioSqlDialect)dremioDialect);
        this.columnProperties = new HashMap<String, Map<String, String>>();
    }
    
    public DremioRelToSqlConverter getDremioRelToSqlConverter() {
        return this.getJdbcDremioRelToSqlConverter();
    }
    
    protected abstract JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter();
    
    public void setColumnProperties(final Map<String, Map<String, String>> columnProperties) {
        this.columnProperties = columnProperties;
    }
    
    public SqlImplementor.Result visit(final Window e) {
        this.windowStack.push(e);
        final SqlImplementor.Result x = this.visitChild(0, e.getInput());
        this.windowStack.pop();
        final SqlImplementor.Builder builder = x.builder((RelNode)e, new SqlImplementor.Clause[0]);
        final RelNode input = e.getInput();
        final List<RexOver> rexOvers = (List<RexOver>)WindowUtil.getOver(e);
        final List<SqlNode> selectList = new ArrayList<SqlNode>();
        if (!(input instanceof JdbcProject) || !((JdbcProject)input).isDummyProject()) {
            for (final RelDataTypeField field : input.getRowType().getFieldList()) {
                this.addSelect((List)selectList, builder.context.field(field.getIndex()), e.getRowType());
            }
        }
        for (final RexOver rexOver : rexOvers) {
            this.addSelect((List)selectList, builder.context.toSql((RexProgram)null, (RexNode)rexOver), e.getRowType());
        }
        builder.setSelect(new SqlNodeList((Collection)selectList, JdbcDremioRelToSqlConverter.POS));
        return builder.result();
    }
    
    public SqlImplementor.Context aliasContext(final Map<String, RelDataType> aliases, final boolean qualified) {
        return (SqlImplementor.Context)new JdbcDremioAliasContext(aliases, qualified);
    }
    
    public SqlImplementor.Context selectListContext(final SqlNodeList selectList) {
        return (SqlImplementor.Context)new JdbcDremioSelectListContext(selectList);
    }
    
    protected SqlNode addCastIfNeeded(final SqlIdentifier expr, final RelDataType type) {
        if (this.shouldAddExplicitCast(expr)) {
            final SqlIdentifier typeIdentifier = new SqlIdentifier(type.getSqlTypeName().name(), SqlParserPos.ZERO);
            final SqlDataTypeSpec spec = new SqlDataTypeSpec(typeIdentifier, type.getPrecision(), type.getScale(), (String)null, (TimeZone)null, SqlParserPos.ZERO);
            return (SqlNode)SqlStdOperatorTable.CAST.createCall(JdbcDremioRelToSqlConverter.POS, new SqlNode[] { expr, spec });
        }
        return (SqlNode)expr;
    }
    
    private boolean shouldAddExplicitCast(final SqlIdentifier node) {
        final String lowerCaseName = ((String)Util.last((List)node.names)).toLowerCase(Locale.ROOT);
        final Map<String, String> properties = this.columnProperties.get(lowerCaseName);
        if (properties != null) {
            final String explicitCast = properties.get("explicitCast");
            return Boolean.TRUE.toString().equals(explicitCast);
        }
        return false;
    }
    
    static /* synthetic */ int access$200(final Map x0) {
        return computeFieldCount(x0);
    }
    
    static /* synthetic */ DremioSqlDialect access$400(final JdbcDremioRelToSqlConverter x0) {
        return x0.getDialect();
    }
    
    static /* synthetic */ boolean access$500(final JdbcDremioRelToSqlConverter x0) {
        return x0.isSubQuery();
    }
    
    public abstract class JdbcDremioContext extends DremioRelToSqlConverter.DremioContext
    {
        protected JdbcDremioContext(final int fieldCount) {
            super(fieldCount);
        }
        
        public SqlNode toSql(final RexProgram program, final RexNode rex) {
            switch (rex.getKind()) {
                case SCALAR_QUERY:
                case EXISTS: {
                    final RexSubQuery subQuery = (RexSubQuery)rex;
                    final SqlOperator subQueryOperator = subQuery.getOperator();
                    JdbcDremioRelToSqlConverter.this.outerQueryAliasContextStack.push(this);
                    DremioRelToSqlConverter.Result subQueryResult;
                    if (subQuery.rel instanceof JdbcTableScan && rex.getKind() == SqlKind.EXISTS) {
                        final JdbcTableScan tableScan = (JdbcTableScan)subQuery.rel;
                        final RexLiteral literalOne = tableScan.getCluster().getRexBuilder().makeBigintLiteral(BigDecimal.ONE);
                        final RelDataTypeFactory.FieldInfoBuilder builder = tableScan.getCluster().getTypeFactory().builder();
                        builder.add("EXPR", literalOne.getType());
                        final JdbcProject project = new JdbcProject(tableScan.getCluster(), tableScan.getTraitSet(), (RelNode)tableScan, (List<? extends RexNode>)ImmutableList.of(literalOne), builder.build(), tableScan.getPluginId());
                        subQueryResult = (DremioRelToSqlConverter.Result)JdbcDremioRelToSqlConverter.this.visitChild(0, (RelNode)project);
                    }
                    else {
                        subQueryResult = (DremioRelToSqlConverter.Result)JdbcDremioRelToSqlConverter.this.visitChild(0, subQuery.rel);
                    }
                    JdbcDremioRelToSqlConverter.this.outerQueryAliasContextStack.pop();
                    final List<SqlNode> operands = (List<SqlNode>)this.toSql(program, subQuery.getOperands());
                    operands.add(subQueryResult.asNode());
                    return (SqlNode)subQueryOperator.createCall(new SqlNodeList((Collection)operands, org.apache.calcite.rel.rel2sql.SqlImplementor.POS));
                }
                default: {
                    return super.toSql(program, rex);
                }
            }
        }
    }
    
    protected class JdbcDremioAliasContext extends JdbcDremioContext
    {
        private final boolean qualified;
        private final Map<String, RelDataType> aliases;
        
        public JdbcDremioAliasContext(final Map<String, RelDataType> aliases, final boolean qualified) {
            super(JdbcDremioRelToSqlConverter.access$200(aliases));
            this.aliases = aliases;
            this.qualified = qualified;
        }
        
        public SqlNode field(int ordinal) {
            for (final Map.Entry<String, RelDataType> alias : this.getAliases().entrySet()) {
                if (ordinal < 0) {
                    break;
                }
                final List<RelDataTypeField> fields = (List<RelDataTypeField>)alias.getValue().getFieldList();
                if (ordinal < fields.size()) {
                    final RelDataTypeField field = fields.get(ordinal);
                    final SqlNode mappedSqlNode = JdbcDremioRelToSqlConverter.this.ordinalMap.get(field.getName().toLowerCase(Locale.ROOT));
                    if (mappedSqlNode != null) {
                        return mappedSqlNode;
                    }
                    final SqlCollation collation = (field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) ? JdbcDremioRelToSqlConverter.access$400(JdbcDremioRelToSqlConverter.this).getDefaultCollation(SqlKind.IDENTIFIER) : null;
                    return (SqlNode)new SqlIdentifier((List)((!this.qualified && !JdbcDremioRelToSqlConverter.access$500(JdbcDremioRelToSqlConverter.this.getJdbcDremioRelToSqlConverter())) ? ImmutableList.of(field.getName()) : ImmutableList.of(alias.getKey(), field.getName())), collation, SqlImplementor.POS, (List)null);
                }
                else {
                    ordinal -= fields.size();
                }
            }
            throw new AssertionError(("field ordinal " + ordinal + " out of range " + this.getAliases()));
        }
        
        public Map<String, RelDataType> getAliases() {
            return this.aliases;
        }
    }
    
    protected class JdbcDremioSelectListContext extends JdbcDremioContext
    {
        private final SqlNodeList selectList;
        
        protected JdbcDremioSelectListContext(final SqlNodeList selectList) {
            super(selectList.size());
            this.selectList = selectList;
        }
        
        public SqlNode field(final int ordinal) {
            final SqlNode selectItem = this.selectList.get(ordinal);
            switch (selectItem.getKind()) {
                case AS: {
                    return ((SqlCall)selectItem).operand(0);
                }
                default: {
                    return selectItem;
                }
            }
        }
    }
}
