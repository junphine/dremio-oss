package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.planner.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.physical.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.rex.*;

public class CollationFilterChecker extends StatelessRelShuttleImpl
{
    private boolean hasCollationFilter;
    
    public CollationFilterChecker() {
        this.hasCollationFilter = false;
    }
    
    public RelNode visit(final LogicalFilter filter) {
        if (null == filter.getCondition().accept((RexVisitor)new CollationRexFilterChecker())) {
            this.hasCollationFilter = true;
            return null;
        }
        return super.visit(filter);
    }
    
    public RelNode visit(final RelNode other) {
        if (other instanceof FilterPrel && null == ((FilterPrel)other).getCondition().accept((RexVisitor)new CollationRexFilterChecker())) {
            this.hasCollationFilter = true;
            return null;
        }
        return super.visit(other);
    }
    
    public boolean hasCollationFilter() {
        return this.hasCollationFilter;
    }
    
    public static boolean hasCollationFilter(final RelNode node) {
        final CollationFilterChecker checker = new CollationFilterChecker();
        node.accept((RelShuttle)checker);
        return checker.hasCollationFilter();
    }
    
    static boolean hasCollationFilter(final RexNode rexCall, final RexNode rexLiteral) {
        return checkForCollationCast(rexCall) && rexLiteral instanceof RexLiteral && rexLiteral.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
    }
    
    public static boolean checkForCollationCast(final RexNode node) {
        if (node instanceof RexCall) {
            final RexCall call = (RexCall)node;
            if (call.getOperator().getKind() == SqlKind.CAST && call.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC) {
                return call.getOperands().get(0).getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
            }
        }
        return false;
    }
    
    public static class CollationRexFilterChecker extends RexShuttle
    {
        private boolean hasCollationFilter;
        
        public CollationRexFilterChecker() {
            this.hasCollationFilter = false;
        }
        
        public RexNode visitCall(final RexCall call) {
            switch (call.getOperator().getKind()) {
                case AND: {
                    if (this.visitCall((RexCall)call.operands.get(0)) == null && this.visitCall((RexCall)call.operands.get(1)) == null) {
                        this.hasCollationFilter = true;
                        return null;
                    }
                    break;
                }
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case EQUALS:
                case NOT_EQUALS: {
                    boolean hasFilter = false;
                    if (call.operands.get(0) instanceof RexCall && call.operands.get(1) instanceof RexLiteral) {
                        hasFilter = CollationFilterChecker.hasCollationFilter((RexNode)call.operands.get(0), (RexNode)call.operands.get(1));
                    }
                    else if (call.operands.get(1) instanceof RexCall && call.operands.get(0) instanceof RexLiteral) {
                        hasFilter = CollationFilterChecker.hasCollationFilter((RexNode)call.operands.get(1), (RexNode)call.operands.get(0));
                    }
                    if (hasFilter) {
                        this.hasCollationFilter = true;
                        return null;
                    }
                    break;
                }
            }
            this.hasCollationFilter = false;
            return super.visitCall(call);
        }
        
        boolean hasCollationFilter() {
            return this.hasCollationFilter;
        }
    }
}
