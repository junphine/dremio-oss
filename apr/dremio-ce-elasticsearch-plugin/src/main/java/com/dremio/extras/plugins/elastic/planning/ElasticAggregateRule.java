package com.dremio.extras.plugins.elastic.planning;

import com.dremio.exec.expr.fn.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.util.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.sql.*;
import com.dremio.plugins.elastic.planning.rels.*;
import com.dremio.plugins.elastic.planning.rules.*;
import com.google.common.base.*;
import com.google.common.collect.*;

import com.dremio.plugins.elastic.*;
import com.dremio.exec.planner.common.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.pojo.*;
import com.dremio.common.expression.*;

import com.dremio.plugins.elastic.mapping.*;
import com.dremio.exec.record.*;
import com.dremio.exec.catalog.*;
import org.slf4j.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import com.dremio.common.types.*;

public class ElasticAggregateRule extends RelOptRule
{
    private static final Logger logger;
    private final FunctionLookupContext functionLookupContext;
    
    public ElasticAggregateRule(final FunctionLookupContext functionLookupContext) {
        super(RelOptHelper.some(AggPrelBase.class, RelOptHelper.any(ElasticsearchIntermediatePrel.class), new RelOptRuleOperand[0]), "ElasticAggregateRule");
        this.functionLookupContext = functionLookupContext;
    }
    
    public void onMatch(final RelOptRuleCall call) {
        final AggPrelBase aggregate = call.rel(0);
        final ElasticsearchIntermediatePrel oldInter = call.rel(1);
        final ElasticsearchAggregate newAggregate = new ElasticsearchAggregate(oldInter.getInput().getCluster(), oldInter.getInput().getTraitSet(), oldInter.getInput(), aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList(), oldInter.getPluginId());
        final ElasticsearchIntermediatePrel newInter = oldInter.withNewInput((ElasticsearchPrel)newAggregate);
        call.transformTo((RelNode)newInter);
    }
    
    public static boolean contains(final List<List<SchemaPath>> sourcesNames, final String columnName) {
        for (final List<SchemaPath> paths : sourcesNames) {
            for (final SchemaPath path : paths) {
                if (path != null && columnName.equals(path.getRootSegment().getPath())) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public static boolean containsMetadata(final List<List<SchemaPath>> sourceNames) {
        for (final List<SchemaPath> paths : sourceNames) {
            if (containsMetadataInput(paths)) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean containsMetadataInput(final List<SchemaPath> paths) {
        for (final SchemaPath path : paths) {
            if (ElasticsearchConstants.META_PATHS.contains(path)) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean countOnMetadataColumn(final Aggregate aggregate, final List<List<SchemaPath>> sourceFieldNames) {
        for (final AggregateCall call : aggregate.getAggCallList()) {
            if (call.getAggregation().getKind() == SqlKind.COUNT && !call.isDistinct()) {
                for (final int argIndex : call.getArgList()) {
                    if (containsMetadataInput(sourceFieldNames.get(argIndex))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    private boolean allowedOperationsOnMetadataColumns(final List<List<SchemaPath>> sourceFieldNames, final Aggregate aggregate, final boolean needsScripts) {
        if (contains(sourceFieldNames, "_id")) {
            ElasticAggregateRule.logger.debug("Cannot pushdown Aggregate that contains _id field, " + aggregate);
            return false;
        }
        if (needsScripts && containsMetadata(sourceFieldNames)) {
            ElasticAggregateRule.logger.debug("Cannot pushdown Aggregate that contains metadata field and requires scripts, " + aggregate);
            return false;
        }
        if (countOnMetadataColumn(aggregate, sourceFieldNames)) {
            ElasticAggregateRule.logger.debug("Cannot pushdown COUNT(metadataColumn), " + aggregate);
            return false;
        }
        return true;
    }
    
    private List<List<SchemaPath>> getSourceFieldNames(final ElasticsearchProject proj, final ElasticIntermediateScanPrel scan) {
        if (proj == null) {
            return FluentIterable.from(scan.getRowType().getFieldNames()).transform(new Function<String, List<SchemaPath>>() {
                public List<SchemaPath> apply(final String input) {
                    return ImmutableList.of(SchemaPath.getSimplePath(input));
                }
            }).toList();
        }
        final List<RexNode> projectExprs = (List<RexNode>)proj.getProjects();
        final List<List<SchemaPath>> sourceFieldNames = new ArrayList<List<SchemaPath>>(projectExprs.size());
        for (final RexNode rexNode : projectExprs) {
            sourceFieldNames.add(ElasticSourceNameFinder.getSchemaPaths(rexNode.accept(new ElasticSourceNameFinder(scan.getRowType()))));
        }
        return sourceFieldNames;
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final AggPrelBase aggregate = call.rel(0);
        final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);
        final ElasticIntermediateScanPrel scan = intermediatePrel.get(ElasticIntermediateScanPrel.class);
        final ElasticsearchConf config = ElasticsearchConf.createElasticsearchConf(scan.getPluginId().getConnectionConf());
        if (intermediatePrel.hasTerminalPrel()) {
            return false;
        }
        final ElasticsearchProject project = intermediatePrel.getNoCheck(ElasticsearchProject.class);
        if (project != null && !project.canPushdown(scan, this.functionLookupContext, ImmutableSet.of())) {
            return false;
        }
        if (!intermediatePrel.getPluginId().getCapabilities().getCapability(ElasticsearchStoragePlugin.SUPPORTS_NEW_FEATURES) && aggregate.getGroupCount() > 1) {
            return false;
        }
        final ElasticsearchPrel prel = (ElasticsearchPrel)intermediatePrel.getInput().accept(new MoreRelOptUtil.SubsetRemover());
        final BatchSchema inputSchema = prel.getSchema(this.functionLookupContext);
        final boolean needsScripts = project != null && project.getNeedsScript();
        final List<List<SchemaPath>> sourceFieldNames = this.getSourceFieldNames(project, scan);
        if (!this.allowedOperationsOnMetadataColumns(sourceFieldNames, (Aggregate)aggregate, needsScripts)) {
            return false;
        }
        final boolean scriptsEnabled = ElasticsearchConf.createElasticsearchConf((BaseElasticStoragePluginConfig)intermediatePrel.getPluginId().getConnectionConf()).isScriptsEnabled();
        for (final AggregateCall aggCall : aggregate.getAggCallList()) {
            final String name;
            final String functionName = name = aggCall.getAggregation().getName();
            switch (name) {
                case "COUNT": {
                    if (!this.canPushdownCount(scriptsEnabled, aggCall, sourceFieldNames)) {
                        return false;
                    }
                    continue;
                }
                case "SUM":
                case "$SUM0":
                case "AVG":
                case "MAX":
                case "MIN":
                case "STDDEV_POP":
                case "STDDEV_SAMP":
                case "STDDEV":
                case "VAR_POP":
                case "VAR_SAMP":
                case "VARIANCE": {
                    final List<Integer> argList = (List<Integer>)aggCall.getArgList();
                    Preconditions.checkArgument(argList.size() == 1);
                    final int argIndex = argList.get(0);
                    final CompleteType type = CompleteType.fromField((Field)inputSchema.getFields().get(argIndex));
                    ElasticAggregateRule.logger.warn(type.toString());
                    switch (type.toMinorType()) {
                        case BIGINT:
                        case FLOAT4:
                        case FLOAT8:
                        case INT:
                        case DATE:
                        case TIMESTAMP:
                        case TIME: {
                            continue;
                        }
                        default: {
                            ElasticAggregateRule.logger.debug("Cannot pushdown Aggregate with aggregate call {}.", aggCall);
                            return false;
                        }
                    }
                    //break;
                }
                default: {
                    ElasticAggregateRule.logger.debug("Cannot pushdown Aggregate with unsupported call, {}", aggCall);
                    return false;
                }
            }
        }
        List<Optional<FieldAnnotation>> annotations;
        if (project != null) {
            annotations = (List<Optional<FieldAnnotation>>)FluentIterable.from((Iterable)project.getChildExps()).transform((Function)new Function<RexNode, Optional<FieldAnnotation>>() {
                public Optional<FieldAnnotation> apply(final RexNode input) {
                    return (Optional<FieldAnnotation>)Optional.fromNullable(scan.getAnnotation(input, ElasticIntermediateScanPrel.IndexMode.SKIP));
                }
            }).toList();
        }
        else {
            annotations = (List<Optional<FieldAnnotation>>)FluentIterable.from((Iterable)inputSchema.getFields()).transform((Function)new Function<Field, Optional<FieldAnnotation>>() {
                public Optional<FieldAnnotation> apply(final Field input) {
                    return (Optional<FieldAnnotation>)Optional.fromNullable(scan.getAnnotation(SchemaPath.getSimplePath(input.getName())));
                }
            }).toList();
        }
        final StoragePluginId pluginId = intermediatePrel.getPluginId();
        final ImmutableBitSet groupSet = aggregate.getGroupSet();
        if (!groupSet.isEmpty()) {
            if (!pluginId.getCapabilities().getCapability(ElasticsearchStoragePlugin.SUPPORTS_NEW_FEATURES) && groupSet.size() > 1) {
                ElasticAggregateRule.logger.debug("Multiple aggregation pushdown is not supported in this version due to elasticsearch bug #15746");
                return false;
            }
            for (final int i : groupSet) {
                final CompleteType type2 = CompleteType.fromField((Field)inputSchema.getFields().get(i));
                switch (type2.toMinorType()) {
                    case BIT: {
                        if (scriptsEnabled) {
                            continue;
                        }
                        ElasticAggregateRule.logger.debug("Cannot pushdown aggregation (group by or distinct) for boolean field without scripts.");
                        return false;
                    }
                    case VARCHAR: {
                        final Optional<FieldAnnotation> annotation = annotations.get(i);
                        final boolean allowGroupByOnNormalizedFields = config.isAllowPushdownOnNormalizedOrAnalyzedFields();
                        if (!annotation.isPresent()) {
                            continue;
                        }
                        if (((FieldAnnotation)annotation.get()).isAnalyzed() || (((FieldAnnotation)annotation.get()).isNormalized() && !allowGroupByOnNormalizedFields)) {
                            ElasticAggregateRule.logger.debug("Cannot pushdown aggregation (group by or distinct) since it is an analyzed text column.");
                            return false;
                        }
                        if (((FieldAnnotation)annotation.get()).isNotIndexed() && ((FieldAnnotation)annotation.get()).isDocValueMissing()) {
                            ElasticAggregateRule.logger.debug("Cannot pushdown aggregation (group by or distinct) since it is a column without index or doc values.");
                            return false;
                        }
                        continue;
                    }
                    case BIGINT:
                    case FLOAT4:
                    case FLOAT8:
                    case INT:
                    case DATE:
                    case TIMESTAMP:
                    case TIME: {
                        continue;
                    }
                    default: {
                        ElasticAggregateRule.logger.debug("Group by on type {} not allowed.", type2);
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    private boolean canPushdownCount(final boolean scriptsEnabled, final AggregateCall aggCall, final List<List<SchemaPath>> sourceFieldNames) {
        if (!scriptsEnabled) {
            for (final int argIndex : aggCall.getArgList()) {
                final List<SchemaPath> fieldNames = sourceFieldNames.get(argIndex);
                if (sourceFieldNames.get(argIndex) != null) {
                    if (sourceFieldNames.get(argIndex).size() == 0) {
                        continue;
                    }
                    for (final SchemaPath fieldName : fieldNames) {
                        if (fieldName != null) {
                            if (fieldName.getAsUnescapedPath().isEmpty()) {
                                continue;
                            }
                            ElasticAggregateRule.logger.debug("Cannot pushdown count(column) when script is not enabled, " + aggCall);
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
    
    static {
        logger = LoggerFactory.getLogger(ElasticAggregateRule.class);
    }
    
    class ElasticProjectFinder extends RelVisitor
    {
        private ElasticsearchProject project;
        
        ElasticProjectFinder() {
            this.project = null;
        }
        
        public ElasticsearchProject getProject(final ElasticIntermediateScanPrel scan) {
            if (this.project == null) {
                final List<RexNode> projectExprs = new ArrayList<RexNode>(scan.getRowType().getFieldCount());
                for (int i = 0; i < scan.getRowType().getFieldCount(); ++i) {
                    projectExprs.add((RexNode)RexInputRef.of(i, scan.getRowType()));
                }
                return new ElasticsearchProject(scan.getCluster(), scan.getTraitSet(), (RelNode)scan, (List)projectExprs, scan.getRowType(), scan.getPluginId());
            }
            return this.project;
        }
        
        public void visit(final RelNode node, final int ordinal, final RelNode parent) {
            if (node instanceof ElasticsearchProject) {
                Preconditions.checkState(this.project == null);
                this.project = (ElasticsearchProject)node;
            }
            super.visit(node, ordinal, parent);
        }
    }
}
