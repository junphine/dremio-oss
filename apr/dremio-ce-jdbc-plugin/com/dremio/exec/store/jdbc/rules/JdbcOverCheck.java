package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.conf.*;
import com.google.common.collect.*;
import org.apache.calcite.rex.*;
import java.util.*;
import org.slf4j.*;

public final class JdbcOverCheck implements RexVisitor<Boolean>
{
    private static final Logger logger;
    private final JdbcDremioSqlDialect dialect;
    
    public static boolean hasOver(final RexNode rex, final StoragePluginId pluginId) {
        final DialectConf<?, ?> conf = (DialectConf<?, ?>)pluginId.getConnectionConf();
        return (boolean)rex.accept((RexVisitor)new JdbcOverCheck(conf.getDialect()));
    }
    
    private JdbcOverCheck(final JdbcDremioSqlDialect dialect) {
        this.dialect = dialect;
    }
    
    public Boolean visitInputRef(final RexInputRef paramRexInputRef) {
        return true;
    }
    
    public Boolean visitLocalRef(final RexLocalRef paramRexLocalRef) {
        return true;
    }
    
    public Boolean visitPatternFieldRef(final RexPatternFieldRef fieldRef) {
        return true;
    }
    
    public Boolean visitTableInputRef(final RexTableInputRef fieldRef) {
        return true;
    }
    
    public Boolean visitLiteral(final RexLiteral paramRexLiteral) {
        return true;
    }
    
    public Boolean visitCall(final RexCall paramRexCall) {
        for (final RexNode operand : paramRexCall.operands) {
            if (!(boolean)operand.accept((RexVisitor)this)) {
                return false;
            }
        }
        return true;
    }
    
    public Boolean visitOver(final RexOver over) {
        JdbcOverCheck.logger.debug("Evaluating if Over clause is supported using supportsOver: {}.", (Object)over);
        final boolean overSupported = this.dialect.supportsOver(over);
        if (!overSupported) {
            JdbcOverCheck.logger.debug("Over clause was not supported. Aborting pushdown.");
            return false;
        }
        JdbcOverCheck.logger.debug("Over clause was supported.");
        return true;
    }
    
    public Boolean visitCorrelVariable(final RexCorrelVariable paramRexCorrelVariable) {
        return true;
    }
    
    public Boolean visitDynamicParam(final RexDynamicParam paramRexDynamicParam) {
        return true;
    }
    
    public Boolean visitRangeRef(final RexRangeRef paramRexRangeRef) {
        return true;
    }
    
    public Boolean visitFieldAccess(final RexFieldAccess paramRexFieldAccess) {
        return (Boolean)paramRexFieldAccess.getReferenceExpr().accept((RexVisitor)this);
    }
    
    public Boolean visitSubQuery(final RexSubQuery subQuery) {
        for (final RexNode operand : subQuery.getOperands()) {
            if (!(boolean)operand.accept((RexVisitor)this)) {
                return false;
            }
        }
        return true;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcOverCheck.class);
    }
}
