package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.store.jdbc.*;
import java.util.function.*;
import org.apache.calcite.util.*;
import com.google.common.collect.*;
import org.apache.calcite.rex.*;
import java.util.*;
import org.apache.calcite.plan.hep.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.store.jdbc.rel.*;

public final class UnpushableTypeVisitor extends RexVisitorImpl<Boolean>
{
    private final Map<String, Map<String, String>> columnProperties;
    private final RelNode rootNode;
    
    private UnpushableTypeVisitor(final RelNode node, final Map<String, Map<String, String>> columnProperties) {
        super(true);
        this.rootNode = node;
        this.columnProperties = columnProperties;
    }
    
    public static boolean hasUnpushableTypes(final RelNode node, final RexNode rexNode) {
        return hasUnpushableTypes(node, (List<RexNode>)ImmutableList.of((Object)rexNode));
    }
    
    public static boolean hasUnpushableTypes(final RelNode node, final List<RexNode> rexNodes) {
        final ColumnPropertyAccumulator accumulator;
        return hasUnpushableTypes(node, rexNodes, () -> {
            accumulator = new ColumnPropertyAccumulator();
            node.accept((RelShuttle)accumulator);
            return accumulator.getColumnProperties();
        });
    }
    
    private static boolean hasUnpushableTypes(final RelNode node, final List<RexNode> rexNodes, final Supplier<Map<String, Map<String, String>>> propertyProducer) {
        UnpushableTypeVisitor visitor = null;
        final Iterator<RexNode> itr = rexNodes.stream().filter(n -> !(n instanceof RexInputRef)).iterator();
        try {
            while (itr.hasNext()) {
                if (null == visitor) {
                    visitor = new UnpushableTypeVisitor(node, propertyProducer.get());
                }
                if (itr.next().accept((RexVisitor)visitor)) {
                    return true;
                }
            }
        }
        catch (Util.FoundOne e) {
            return (boolean)e.getNode();
        }
        return false;
    }
    
    public Boolean visitInputRef(final RexInputRef inputRef) {
        final int refIndex = inputRef.getIndex();
        final UnpushableRelVisitor finder = new UnpushableRelVisitor(refIndex, this.columnProperties);
        finder.go(this.rootNode);
        if (finder.hasUnpushableTypes()) {
            throw new Util.FoundOne((Object)Boolean.TRUE);
        }
        return false;
    }
    
    public Boolean visitOver(final RexOver over) {
        final RexWindow window = over.getWindow();
        for (final RexFieldCollation orderKey : window.orderKeys) {
            if (((RexNode)orderKey.left).accept((RexVisitor)this)) {
                throw new Util.FoundOne((Object)Boolean.TRUE);
            }
        }
        for (final RexNode partitionKey : window.partitionKeys) {
            if (partitionKey.accept((RexVisitor)this)) {
                throw new Util.FoundOne((Object)Boolean.TRUE);
            }
        }
        return false;
    }
    
    public Boolean visitLocalRef(final RexLocalRef localRef) {
        return hasUnpushableType(localRef.getName(), this.columnProperties);
    }
    
    public Boolean visitLiteral(final RexLiteral literal) {
        return false;
    }
    
    public Boolean visitCorrelVariable(final RexCorrelVariable correlVariable) {
        return false;
    }
    
    public Boolean visitCall(final RexCall call) {
        if (this.deep) {
            for (final RexNode node : call.getOperands()) {
                if (node.accept((RexVisitor)this)) {
                    throw new Util.FoundOne((Object)Boolean.TRUE);
                }
            }
        }
        return false;
    }
    
    public Boolean visitDynamicParam(final RexDynamicParam dynamicParam) {
        return false;
    }
    
    public Boolean visitPatternFieldRef(final RexPatternFieldRef fieldRef) {
        return false;
    }
    
    public Boolean visitRangeRef(final RexRangeRef rangeRef) {
        return false;
    }
    
    public Boolean visitSubQuery(final RexSubQuery subQuery) {
        final UnpushableTypeVisitor visitor = new UnpushableTypeVisitor(subQuery.rel, this.columnProperties);
        for (final RexNode node : subQuery.rel.getChildExps()) {
            if (node.accept((RexVisitor)visitor)) {
                throw new Util.FoundOne((Object)Boolean.TRUE);
            }
        }
        return false;
    }
    
    public static Boolean hasUnpushableType(final String name, final Map<String, Map<String, String>> columnProperties) {
        final Map<String, String> colProperties = columnProperties.get(name.toLowerCase(Locale.ROOT));
        if (null != colProperties) {
            return Boolean.parseBoolean(colProperties.get("unpushable"));
        }
        return false;
    }
    
    private static class UnpushableRelVisitor extends RelVisitor
    {
        private final Map<String, Map<String, String>> columnProperties;
        private int curColumnIndex;
        private boolean hasUnpushableTypes;
        
        UnpushableRelVisitor(final int index, final Map<String, Map<String, String>> columnProperties) {
            this.curColumnIndex = index;
            this.columnProperties = columnProperties;
            this.hasUnpushableTypes = false;
        }
        
        public void visit(RelNode node, final int ordinal, final RelNode parent) {
            if (node instanceof HepRelVertex) {
                node = ((HepRelVertex)node).getCurrentRel();
            }
            if (node instanceof JdbcJoin) {
                final JdbcJoin join = (JdbcJoin)node;
                if (join.getLeft().getRowType().getFieldCount() > this.curColumnIndex) {
                    this.visit(join.getLeft(), ordinal, node);
                }
                else {
                    this.curColumnIndex -= join.getLeft().getRowType().getFieldCount();
                    this.visit(join.getRight(), ordinal, node);
                }
            }
            else if (node instanceof JdbcProject || node instanceof JdbcAggregate) {
                final SingleRel singleRel = (SingleRel)node;
                this.hasUnpushableTypes |= hasUnpushableTypes(singleRel.getInput(), singleRel.getChildExps(), () -> this.columnProperties);
            }
            else if (node instanceof JdbcTableScan || node instanceof JdbcWindow) {
                final String colName = node.getRowType().getFieldNames().get(this.curColumnIndex);
                this.hasUnpushableTypes |= UnpushableTypeVisitor.hasUnpushableType(colName, this.columnProperties);
            }
            else {
                super.visit(node, ordinal, parent);
            }
        }
        
        public Boolean hasUnpushableTypes() {
            return this.hasUnpushableTypes;
        }
    }
}
