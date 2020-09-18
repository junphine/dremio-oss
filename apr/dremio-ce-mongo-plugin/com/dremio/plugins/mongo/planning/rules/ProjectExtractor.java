package com.dremio.plugins.mongo.planning.rules;

import com.google.common.collect.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.type.*;
import java.util.*;
import org.slf4j.*;

@Deprecated
public class ProjectExtractor extends RexShuttle
{
    private static final Logger logger;
    private final RelDataType recordType;
    private final RelDataTypeFactory dataTypeFactory;
    private final boolean mongo3_2enabled;
    private final boolean mongo3_4enabled;
    private final List<RexNode> projectColExprs;
    private final List<RelDataTypeField> projectCols;
    private int currentTempColIndex;
    
    public ProjectExtractor(final RelDataType recordType, final RelDataTypeFactory dataTypeFactory, final boolean isMongo3_2enabled, final boolean isMongo3_4enabled) {
        this.projectColExprs = (List<RexNode>)Lists.newArrayList();
        this.projectCols = (List<RelDataTypeField>)Lists.newArrayList();
        this.currentTempColIndex = 0;
        this.recordType = recordType;
        this.dataTypeFactory = dataTypeFactory;
        this.mongo3_2enabled = isMongo3_2enabled;
        this.mongo3_4enabled = isMongo3_4enabled;
    }
    
    public RexNode visitCall(final RexCall call) {
        final String funcName = call.getOperator().getName().toLowerCase();
        final MongoFunctions function = MongoFunctions.getMongoOperator(funcName);
        if (function != null) {
            if (function.isProjectOnly()) {
                return this.createProject(call);
            }
            final List<RexNode> operands = (List<RexNode>)call.getOperands();
            switch (function) {
                case NOT: {
                    return this.createProject(call);
                }
                case EQUAL:
                case NOT_EQUAL:
                case GREATER:
                case GREATER_OR_EQUAL:
                case LESS:
                case LESS_OR_EQUAL: {
                    final RexNode firstArg = operands.get(0);
                    final RexNode secondArg = operands.get(1);
                    if ((MongoRulesUtil.isSupportedCast(firstArg) && MongoRulesUtil.isLiteral(secondArg)) || (MongoRulesUtil.isLiteral(firstArg) && MongoRulesUtil.isSupportedCast(secondArg))) {
                        return (RexNode)call;
                    }
                }
                case REGEX: {
                    assert operands.size() == 2;
                    final RexNode first = operands.get(0);
                    final RexNode second = operands.get(1);
                    if ((!MongoRulesUtil.isInputRef(first) || !MongoRulesUtil.isLiteral(second)) && (!MongoRulesUtil.isInputRef(second) || !MongoRulesUtil.isLiteral(first))) {
                        return this.createProject(call);
                    }
                    return (RexNode)call;
                }
            }
        }
        return super.visitCall(call);
    }
    
    private RexNode createProject(final RexCall call) {
        final ProjectExpressionConverter visitor = new ProjectExpressionConverter(this.recordType, this.mongo3_2enabled, this.mongo3_4enabled);
        final Object projectColDef = call.accept((RexVisitor)visitor);
        final int fieldId = this.recordType.getFieldCount() + this.currentTempColIndex++;
        final RelDataTypeField newProjectField = (RelDataTypeField)new RelDataTypeFieldImpl("__temp" + fieldId, fieldId, this.dataTypeFactory.createSqlType(SqlTypeName.ANY));
        this.projectColExprs.add((RexNode)call);
        this.projectCols.add(newProjectField);
        return (RexNode)new RexInputRef(newProjectField.getIndex(), newProjectField.getType());
    }
    
    public boolean hasNewProjects() {
        return this.projectCols.size() > 0 && this.projectColExprs.size() > 0;
    }
    
    public RelDataType getNewRecordType() {
        final List<RelDataTypeField> fields = (List<RelDataTypeField>)Lists.newArrayList((Iterable)this.recordType.getFieldList());
        fields.addAll(this.projectCols);
        return (RelDataType)new RelRecordType((List)fields);
    }
    
    public List<RexNode> getOuterProjects() {
        final List<RexNode> projectExpr = (List<RexNode>)Lists.newArrayList();
        int index = 0;
        for (final RelDataTypeField field : this.recordType.getFieldList()) {
            projectExpr.add((RexNode)new RexInputRef(index, field.getType()));
            ++index;
        }
        return projectExpr;
    }
    
    public RelDataType getOuterProjectRecordType() {
        final List<RelDataTypeField> fields = (List<RelDataTypeField>)Lists.newArrayList();
        int index = 0;
        for (final RelDataTypeField field : this.recordType.getFieldList()) {
            fields.add((RelDataTypeField)new RelDataTypeFieldImpl(field.getName(), index, field.getType()));
            ++index;
        }
        return (RelDataType)new RelRecordType((List)fields);
    }
    
    public List<RexNode> getProjects() {
        final List<RexNode> projectExpr = (List<RexNode>)Lists.newArrayList();
        for (final RelDataTypeField field : this.recordType.getFieldList()) {
            projectExpr.add((RexNode)new RexInputRef(field.getIndex(), field.getType()));
        }
        projectExpr.addAll(this.projectColExprs);
        return projectExpr;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)ProjectExtractor.class);
    }
}
