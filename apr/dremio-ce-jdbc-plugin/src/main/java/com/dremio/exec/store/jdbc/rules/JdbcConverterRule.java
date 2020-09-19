package com.dremio.exec.store.jdbc.rules;

import java.util.*;
import java.lang.ref.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.tools.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.store.jdbc.conf.*;
import java.util.concurrent.*;
import com.google.common.cache.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.planner.sql.handlers.*;

abstract class JdbcConverterRule extends RelOptRule
{
    private static final Map<WeakReference<StoragePluginId>, RuleContext> ruleContextCache;
    
    JdbcConverterRule(final RelOptRuleOperand operand, final String description) {
        super(operand, description);
    }
    
    JdbcConverterRule(final RelOptRuleOperand operand, final RelBuilderFactory relBuilderFactory, final String description) {
        super(operand, relBuilderFactory, description);
    }
    
    protected static JdbcDremioSqlDialect getDialect(final StoragePluginId pluginId) {
        final DialectConf<?, ?> conf = (DialectConf<?, ?>)pluginId.getConnectionConf();
        return conf.getDialect();
    }
    
    static RuleContext getRuleContext(final StoragePluginId pluginId, final RexBuilder builder) {
        RuleContext context = JdbcConverterRule.ruleContextCache.get(new WeakReference(pluginId));
        if (context == null) {
            context = new RuleContext(pluginId, builder);
            JdbcConverterRule.ruleContextCache.put(new WeakReference<StoragePluginId>(pluginId), context);
        }
        return context;
    }
    
    static {
        ruleContextCache = new ConcurrentHashMap<WeakReference<StoragePluginId>, RuleContext>();
    }
    
    static class RuleContext
    {
        private final StoragePluginId pluginId;
        private final RexBuilder builder;
        private final LoadingCache<RexNode, Boolean> subqueryHasSamePluginId;
        private final LoadingCache<RexNode, Boolean> overCheckedExpressions;
        private final LoadingCache<RexNode, Boolean> supportedExpressions;
        
        RuleContext(final StoragePluginId pluginId, final RexBuilder builder) {
            this.subqueryHasSamePluginId = (LoadingCache<RexNode, Boolean>)CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build((CacheLoader)new CacheLoader<RexNode, Boolean>() {
                public Boolean load(final RexNode expr) {
                    if (expr instanceof RexSubQuery) {
                        return new RexSubQueryUtils.RexSubQueryPushdownChecker(RuleContext.this.getPluginId()).canPushdownRexSubQuery();
                    }
                    return true;
                }
            });
            this.overCheckedExpressions = (LoadingCache<RexNode, Boolean>)CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build((CacheLoader)new CacheLoader<RexNode, Boolean>() {
                public Boolean load(final RexNode expr) {
                    return JdbcOverCheck.hasOver(expr, RuleContext.this.getPluginId());
                }
            });
            this.supportedExpressions = (LoadingCache<RexNode, Boolean>)CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build((CacheLoader)new CacheLoader<RexNode, Boolean>() {
                public Boolean load(final RexNode expr) {
                    return JdbcExpressionSupportCheck.hasOnlySupportedFunctions(expr, RuleContext.this.getPluginId(), RuleContext.this.builder);
                }
            });
            this.pluginId = pluginId;
            this.builder = builder;
        }
        
        StoragePluginId getPluginId() {
            return this.pluginId;
        }
        
        LoadingCache<RexNode, Boolean> getSubqueryHasSamePluginId() {
            return this.subqueryHasSamePluginId;
        }
        
        LoadingCache<RexNode, Boolean> getOverCheckedExpressions() {
            return this.overCheckedExpressions;
        }
        
        LoadingCache<RexNode, Boolean> getSupportedExpressions() {
            return this.supportedExpressions;
        }
    }
}
