package com.dremio.extras.plugins.elastic.planning;

import com.dremio.exec.record.*;
import com.dremio.exec.physical.base.*;
import com.dremio.exec.store.*;
import com.dremio.exec.expr.fn.*;
import org.apache.calcite.rex.*;
import com.google.common.collect.*;
import com.google.common.base.*;
import com.dremio.exec.catalog.*;
import org.elasticsearch.script.*;
import com.dremio.plugins.elastic.mapping.*;
import com.dremio.elastic.proto.*;
import org.apache.calcite.rel.type.*;
import java.util.*;
import com.dremio.exec.planner.common.*;
import com.dremio.plugins.elastic.planning.*;
import com.dremio.common.exceptions.*;
import org.apache.calcite.rel.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.apache.calcite.rel.core.*;
import org.elasticsearch.search.aggregations.metrics.cardinality.*;
import com.dremio.plugins.elastic.planning.rules.*;
import org.apache.calcite.util.*;
import org.elasticsearch.search.aggregations.*;
import com.dremio.plugins.elastic.planning.rels.*;
import org.slf4j.*;
import com.dremio.common.expression.*;
import com.dremio.plugins.elastic.*;
import org.joda.time.*;
import com.dremio.common.types.*;

class AggregateScanBuilder extends ScanBuilder
{
    private static final ImmutableSet<Class<?>> CONSUMEABLE_RELS;
    private static final Logger logger;
    private static final Joiner DOT;
    private List<ElasticsearchAggExpr> aggregates;
    private BatchSchema schema;
    
    public GroupScan<SplitWork> toGroupScan(final OpProps props, final long estimatedRowCount) {
        return (GroupScan<SplitWork>)new ElasticsearchAggregatorGroupScan(props, this.getScan().getTableMetadata(), this.aggregates, this.getSpec(), estimatedRowCount, this.schema);
    }
    
    public List<SchemaPath> getColumns() {
        return (List<SchemaPath>)GroupScan.ALL_COLUMNS;
    }
    
    private Map<Class<?>, ElasticsearchPrel> validate(final List<ElasticsearchPrel> stack) {
        final Map<Class<?>, ElasticsearchPrel> map = new HashMap<Class<?>, ElasticsearchPrel>();
        for (int i = 0; i < stack.size(); ++i) {
            final ElasticsearchPrel prel = stack.get(i);
            if (!AggregateScanBuilder.CONSUMEABLE_RELS.contains(prel.getClass())) {
                throw new IllegalStateException(String.format("ScanBuilder can't consume a %s.", prel.getClass().getName()));
            }
            if (map.containsKey(prel.getClass())) {
                throw new IllegalStateException(String.format("ScanBuilder found more than one %s.", prel.getClass().getName()));
            }
            map.put(prel.getClass(), prel);
        }
        switch (stack.size()) {
            case 2: {
                Preconditions.checkArgument(stack.get(0) instanceof ElasticsearchAggregate);
                Preconditions.checkArgument(stack.get(1) instanceof ElasticIntermediateScanPrel);
                break;
            }
            case 3: {
                Preconditions.checkArgument(stack.get(0) instanceof ElasticsearchAggregate);
                Preconditions.checkArgument(stack.get(1) instanceof ElasticsearchProject || stack.get(1) instanceof ElasticsearchFilter);
                Preconditions.checkArgument(stack.get(2) instanceof ElasticIntermediateScanPrel);
                break;
            }
            case 4: {
                Preconditions.checkArgument(stack.get(0) instanceof ElasticsearchAggregate);
                Preconditions.checkArgument(stack.get(1) instanceof ElasticsearchProject);
                Preconditions.checkArgument(stack.get(2) instanceof ElasticsearchFilter);
                Preconditions.checkArgument(stack.get(3) instanceof ElasticIntermediateScanPrel);
                break;
            }
            default: {
                throw new IllegalStateException(String.format("Stack should 2..4 in size, was %d in size.", stack.size()));
            }
        }
        return (Map<Class<?>, ElasticsearchPrel>)ImmutableMap.copyOf((Map)map);
    }
    
    private static List<ProjectResult> getProjectOutcome(final ElasticsearchProject project, final ElasticIntermediateScanPrel scan, final FunctionLookupContext lookupContext) {
        BatchSchema schema;
        List<RexNode> nodes;
        List<String> outputNames;
        if (project != null) {
            schema = project.getSchema(lookupContext);
            nodes = (List<RexNode>)project.getChildExps();
            outputNames = (List<String>)project.getRowType().getFieldNames();
            final RelDataType inputRowType = project.getInput().getRowType();
        }
        else {
            schema = scan.getSchema(lookupContext);
            nodes = new ArrayList<RexNode>();
            outputNames = (List<String>)scan.getRowType().getFieldNames();
            final RelDataType inputRowType = null;
            int i = 0;
            for (final RelDataTypeField f : scan.getRowType().getFieldList()) {
                nodes.add((RexNode)scan.getCluster().getRexBuilder().makeInputRef(f.getType(), i));
                ++i;
            }
        }
        final StoragePluginId pluginId = scan.getPluginId();
        final boolean supportsV5Features = pluginId.getCapabilities().getCapability(ElasticsearchStoragePlugin.ENABLE_V5_FEATURES);
        return (List<ProjectResult>)FluentIterable.from((Iterable)nodes).transform((Function)new Function<RexNode, ProjectResult>() {
            private int i = 0;
            
            public ProjectResult apply(final RexNode original) {
                SchemaPath path = scan.getDirectReferenceIfPossible(original, ElasticIntermediateScanPrel.IndexMode.DISALLOW);
                final ElasticReaderProto.ElasticSpecialType specialType = scan.getSpecialTypeRecursive(path);
                if (specialType != null && SchemaField.NON_DOC_TYPES.contains(specialType)) {
                    path = null;
                }
                final CompleteType outputType = CompleteType.fromField(schema.getColumn(this.i));
                if (outputType.isBoolean()) {
                    path = null;
                }
                final String outputName = outputNames.get(this.i);
                ++this.i;
                if (path != null) {
                    final FieldAnnotation annotation = scan.getAnnotation(path);
                    final ElasticsearchConf config = ElasticsearchConf.createElasticsearchConf((BaseElasticStoragePluginConfig)scan.getPluginId().getConnectionConf());
                    final boolean allowGroupByOnNormalizedFields = config.isAllowPushdownOnNormalizedOrAnalyzedFields();
                    final boolean isAnalyzedOrNormalized = annotation != null && (annotation.isAnalyzed() || (annotation.isNormalized() && !allowGroupByOnNormalizedFields));
                    final boolean isBooleanBeforeV5 = outputType.isBoolean() && !supportsV5Features;
                    final boolean isIPTypeBeforeV5 = annotation != null && annotation.isIpType() && !supportsV5Features;
                    if (!isAnalyzedOrNormalized && !isBooleanBeforeV5 && !isIPTypeBeforeV5) {
                        return new ProjectResult(outputType, false, (Script)null, path, outputName, pluginId, annotation);
                    }
                }
                final RexNode converted = SchemaField.convert(original, scan);
                final ElasticsearchConf config = ElasticsearchConf.createElasticsearchConf((BaseElasticStoragePluginConfig)pluginId.getConnectionConf());
                final Script script = ProjectAnalyzer.getScript(converted, config.isUsePainless(), supportsV5Features, true, true, config.isAllowPushdownOnNormalizedOrAnalyzedFields(), false);
                return new ProjectResult(outputType, true, script, path, outputName, pluginId, (FieldAnnotation)null);
            }
        }).toList();
    }
    
    public void setup(List<ElasticsearchPrel> stack, final FunctionLookupContext functionLookupContext) {
        final RelNode newStackRoot = stack.get(0).accept((RelShuttle)new MoreRelOptUtil.NodeRemover(i -> !(i instanceof ElasticsearchSample)));
        stack = (List<ElasticsearchPrel>)StackFinder.getStack(newStackRoot);
        this.validate(stack);
        try {
            final SearchRequestBuilder searchRequest = this.buildRequestBuilder();
            final Map<Class<?>, ElasticsearchPrel> map = this.validate(stack);
            final ElasticIntermediateScanPrel scan = (ElasticIntermediateScanPrel)map.get(ElasticIntermediateScanPrel.class);
            final ElasticReaderProto.ElasticTableXattr tableAttributes = scan.getExtendedAttributes();
            final ElasticsearchFilter filter = (ElasticsearchFilter)map.get(ElasticsearchFilter.class);
            final ElasticsearchProject project = (ElasticsearchProject)map.get(ElasticsearchProject.class);
            final ElasticsearchAggregate aggregate = (ElasticsearchAggregate)map.get(ElasticsearchAggregate.class);
            this.applyFilter(searchRequest, scan, filter, tableAttributes);
            this.aggregates = this.applyAggregation(searchRequest, scan, aggregate, project, functionLookupContext);
            this.schema = aggregate.getSchema(functionLookupContext);
            searchRequest.setSize(0);
            final ElasticsearchScanSpec scanSpec = new ElasticsearchScanSpec(tableAttributes.getResource(), searchRequest.toString(), ElasticsearchConf.createElasticsearchConf((BaseElasticStoragePluginConfig)scan.getPluginId().getConnectionConf()).getScrollSize(), true);
            this.setSpec(scanSpec);
            this.setScan(scan);
        }
        catch (ExpressionNotAnalyzableException e) {
            throw UserException.dataReadError((Throwable)e).message("Elastic pushdown failed. Too late to recover query.", new Object[0]).build(AggregateScanBuilder.logger);
        }
    }
    
    public List<ElasticsearchAggExpr> applyAggregation(final SearchRequestBuilder requestBuilder, final ElasticIntermediateScanPrel scan, final ElasticsearchAggregate aggregate, final ElasticsearchProject project, final FunctionLookupContext lookupContext) {
        final List<ProjectResult> projectResults = getProjectOutcome(project, scan, lookupContext);
        final ImmutableBitSet group = aggregate.getGroupSet();
        final List<ProjectResult> groups = new ArrayList<ProjectResult>(group.cardinality());
        for (int index = group.nextSetBit(0); index != -1; index = group.nextSetBit(index + 1)) {
            groups.add(projectResults.get(index));
        }
        TermsAggregationBuilder termBuilder = null;
        TermsAggregationBuilder innerMostTermsAggregationBuilder = null;
        if (group.cardinality() > 0) {
            for (int groupIndex = groups.size() - 1; groupIndex >= 0; --groupIndex) {
                final ProjectResult result = groups.get(groupIndex);
                TermsAggregationBuilder newTermBuilder = (TermsAggregationBuilder)AggregationBuilders.terms(result.getOutputName()).size(Integer.MAX_VALUE).missing(result.getMissingType());
                if (result.isRequiresScript()) {
                    newTermBuilder = (TermsAggregationBuilder)newTermBuilder.script(result.getScript());
                }
                else {
                    newTermBuilder = (TermsAggregationBuilder)newTermBuilder.field(result.getReference());
                }
                if (innerMostTermsAggregationBuilder == null) {
                    innerMostTermsAggregationBuilder = newTermBuilder;
                }
                if (termBuilder != null) {
                    newTermBuilder = (TermsAggregationBuilder)newTermBuilder.subAggregation((AggregationBuilder)termBuilder);
                }
                termBuilder = newTermBuilder;
            }
        }
        final ElasticsearchConf config = ElasticsearchConf.createElasticsearchConf((BaseElasticStoragePluginConfig)scan.getPluginId().getConnectionConf());
        final boolean isV5 = scan.getPluginId().getCapabilities().getCapability(ElasticsearchStoragePlugin.ENABLE_V5_FEATURES);
        final boolean enableScripts = config.isScriptsEnabled();
        final boolean isPainless = isV5 && config.isUsePainless();
        final List<ElasticsearchAggExpr> aggExprList = new ArrayList<ElasticsearchAggExpr>();
        for (final Pair<AggregateCall, String> agg : aggregate.getNamedAggCalls()) {
            final AggregateCall aggCall = (AggregateCall)agg.getKey();
            final String operation = aggCall.getAggregation().getName();
            final String outputName = (String)agg.getValue();
            int argIndex = -1;
            if (aggCall.getArgList() != null && aggCall.getArgList().size() > 0) {
                argIndex = aggCall.getArgList().get(0);
            }
            if (argIndex < 0) {
                Preconditions.checkState(operation.equals("COUNT"), ("Aggregation type COUNT expected, but got " + operation));
                Preconditions.checkState(!aggCall.isDistinct(), "Star aggregation count(*) cannot be distinct");
                aggExprList.add(new ElasticsearchAggExpr(operation, ElasticsearchAggExpr.Type.COUNT_ALL));
            }
            else {
                final ProjectResult result2 = projectResults.get(argIndex);
                final String s = operation;
                AbstractAggregationBuilder aggBuilder = null;
                switch (s) {
                    case "SUM":
                    case "$SUM0": {
                        if (result2.isRequiresScript()) {
                            aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.sum(outputName).script(result2.getScript());
                            break;
                        }
                        aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.sum(outputName).field(result2.getReference());
                        break;
                    }
                    case "COUNT": {
                        if (aggCall.isDistinct()) {
                            if (result2.isRequiresScript()) {
                                aggBuilder = (AbstractAggregationBuilder)((CardinalityAggregationBuilder)AggregationBuilders.cardinality(outputName).script(result2.getScript())).precisionThreshold(40000L);
                                break;
                            }
                            aggBuilder = (AbstractAggregationBuilder)((CardinalityAggregationBuilder)AggregationBuilders.cardinality(outputName).field(result2.getReference())).precisionThreshold(40000L);
                            break;
                        }
                        else {
                            if (result2.isRequiresScript()) {
                                aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.count(outputName).script(result2.getScript());
                                break;
                            }
                            if (result2.isSingleLevelReference()) {
                                aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.count(outputName).field(result2.getReference());
                                break;
                            }
                            if (!enableScripts) {
                                throw UserException.permissionError().message("Group by on complex is only allowed with scripts enabled, column: %s", new Object[] { result2.getReference() }).build(AggregateScanBuilder.logger);
                            }
                            final String src = isPainless ? "params._source" : "_source";
                            final String scriptText = "((" + src + "." + result2.getReference() + " == null) ? null : 1)";
                            aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.count(outputName).script(PredicateAnalyzer.getScript(scriptText, scan.getPluginId()));
                            break;
                        }
                        //-break;
                    }
                    case "AVG": {
                        if (result2.isRequiresScript()) {
                            aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.avg(outputName).script(result2.getScript());
                            break;
                        }
                        aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.avg(outputName).field(result2.getReference());
                        break;
                    }
                    case "MAX": {
                        if (result2.isRequiresScript()) {
                            aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.max(outputName).script(result2.getScript());
                            break;
                        }
                        aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.max(outputName).field(result2.getReference());
                        break;
                    }
                    case "MIN": {
                        if (result2.isRequiresScript()) {
                            aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.min(outputName).script(result2.getScript());
                            break;
                        }
                        aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.min(outputName).field(result2.getReference());
                        break;
                    }
                    case "STDDEV":
                    case "STDDEV_POP":
                    case "STDDEV_SAMP":
                    case "VARIANCE":
                    case "VAR_POP":
                    case "VAR_SAMP": {
                        if (result2.isRequiresScript()) {
                            aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.extendedStats(outputName).script(result2.getScript());
                            break;
                        }
                        aggBuilder = (AbstractAggregationBuilder)AggregationBuilders.extendedStats(outputName).field(result2.getReference());
                        break;
                    }
                    default: {
                        throw new RuntimeException("Unexpected aggregation " + operation + ", should not be pushed down to elasticsearch.");
                    }
                }
                if (innerMostTermsAggregationBuilder != null) {
                    innerMostTermsAggregationBuilder.subAggregation((AggregationBuilder)aggBuilder);
                }
                else {
                    requestBuilder.addAggregation((AggregationBuilder)aggBuilder);
                }
                aggExprList.add(new ElasticsearchAggExpr(operation, aggCall.isDistinct() ? ElasticsearchAggExpr.Type.COUNT_DISTINCT : ElasticsearchAggExpr.Type.NORMAL));
            }
        }
        if (innerMostTermsAggregationBuilder != null) {
            Preconditions.checkState(termBuilder != null);
            requestBuilder.addAggregation((AggregationBuilder)termBuilder);
        }
        return aggExprList;
    }
    
    static {
        CONSUMEABLE_RELS = ImmutableSet.of(ElasticsearchProject.class, ElasticsearchAggregate.class, ElasticsearchSample.class, ElasticsearchLimit.class, ElasticsearchFilter.class, ElasticIntermediateScanPrel.class, new Class[0]);
        logger = LoggerFactory.getLogger(AggregateScanBuilder.class);
        DOT = Joiner.on('.');
    }
    
    private static class ProjectResult
    {
        private final CompleteType type;
        private final boolean requiresScript;
        private final Script script;
        private final SchemaPath reference;
        private final String outputName;
        private final StoragePluginId pluginId;
        private final FieldAnnotation annotation;
        
        private ProjectResult(final CompleteType type, final boolean requiresScript, final Script script, final SchemaPath reference, final String outputName, final StoragePluginId pluginId, final FieldAnnotation annotation) {
            this.type = type;
            this.requiresScript = requiresScript;
            this.script = script;
            this.reference = reference;
            this.outputName = outputName;
            this.pluginId = pluginId;
            this.annotation = annotation;
        }
        
        public CompleteType getType() {
            return this.type;
        }
        
        public boolean isRequiresScript() {
            return this.requiresScript;
        }
        
        public Script getScript() {
            Preconditions.checkArgument(this.requiresScript);
            return this.script;
        }
        
        public boolean isSingleLevelReference() {
            Preconditions.checkArgument(!this.requiresScript);
            return this.reference.getRootSegment().isLastPath();
        }
        
        public String getReference() {
            Preconditions.checkArgument(!this.requiresScript);
            PathSegment.NameSegment segment = this.reference.getRootSegment().getNameSegment();
            final List<String> segments = new ArrayList<String>();
            while (true) {
                segments.add(segment.getPath());
                final PathSegment child = segment.getChild();
                if (child == null) {
                    return AggregateScanBuilder.DOT.join((Iterable)segments);
                }
                if (child.isArray()) {
                    throw UserException.validationError().message("Attempted to aggregate %s which includes array indices.", new Object[] { this.reference.getAsUnescapedPath() }).build(AggregateScanBuilder.logger);
                }
                segment = child.getNameSegment();
            }
        }
        
        public Object getMissingType() {
            switch (this.type.toMinorType()) {
                case BIGINT: {
                    return ElasticsearchConstants.NULL_LONG_TAG;
                }
                case BIT: {
                    return "NULL_BOOLEAN_TAG";
                }
                case FLOAT4: {
                    return ElasticsearchConstants.NULL_DOUBLE_TAG;
                }
                case FLOAT8: {
                    return ElasticsearchConstants.NULL_DOUBLE_TAG;
                }
                case INT: {
                    return ElasticsearchConstants.NULL_INTEGER_TAG;
                }
                case DATE:
                case TIME:
                case TIMESTAMP: {
                    final List<String> formats = (List<String>)((this.annotation == null) ? null : this.annotation.getDateFormats());
                    if (formats != null && !formats.isEmpty()) {
                        return DateFormats.getFormatterAndType((String)formats.get(0)).print(new LocalDateTime(ElasticsearchConstants.NULL_TIME_TAG, DateTimeZone.UTC));
                    }
                    return ElasticsearchConstants.NULL_TIME_TAG;
                }
                case VARBINARY: {
                    return ElasticsearchConstants.NULL_BYTE_TAG;
                }
                case VARCHAR: {
                    return "NULL_STRING_TAG";
                }
                default: {
                    throw UserException.validationError().message("Tried to pushdown group by of %s, this isn't currently supported.", new Object[] { this.type }).build(AggregateScanBuilder.logger);
                }
            }
        }
        
        public String getOutputName() {
            return this.outputName;
        }
    }
}
