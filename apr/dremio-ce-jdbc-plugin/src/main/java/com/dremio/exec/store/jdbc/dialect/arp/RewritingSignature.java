package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.arp.transformer.*;
import com.google.common.collect.*;
import org.apache.calcite.sql.pretty.*;
import org.apache.calcite.sql.*;
import java.util.*;
import java.text.*;

class RewritingSignature extends Signature
{
    private final String rewrite;
    
    RewritingSignature(final String returnType, final List<String> args, final String rewrite) {
        super(returnType, args);
        this.rewrite = rewrite;
    }
    
    @Override
    boolean hasRewrite() {
        return true;
    }
    
    @Override
    public String toString() {
        return this.rewrite;
    }
    
    @Override
    public void unparse(final SqlCall originalNode, final CallTransformer transformer, final SqlWriter writer, final int leftPrec, final int rightPrec) {
        final SqlOperator operator = originalNode.getOperator();
        if (leftPrec <= operator.getLeftPrec() && (operator.getRightPrec() > rightPrec || rightPrec == 0) && (!writer.isAlwaysUseParentheses() || !originalNode.isA(SqlKind.EXPRESSION))) {
            this.doUnparse(originalNode, transformer, writer);
        }
        else {
            final SqlWriter.Frame frame = writer.startList("(", ")");
            this.unparse(originalNode, transformer, writer, 0, 0);
            writer.endList(frame);
        }
    }
    
    protected List<String> getOperatorsAsStringList(final SqlCall originalNode, final CallTransformer transformer, final SqlWriter writer) {
        final ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
        final SqlPrettyWriter tempWriter = new SqlPrettyWriter(writer.getDialect());
        tempWriter.setAlwaysUseParentheses(false);
        tempWriter.setSelectListItemsOnSeparateLines(false);
        tempWriter.setIndentation(0);
        for (final SqlNode operand : transformer.transformSqlOperands(originalNode.getOperandList())) {
            tempWriter.reset();
            operand.unparse((SqlWriter)tempWriter, 0, 0);
            argsBuilder.add(tempWriter.toString());
        }
        return (List<String>)argsBuilder.build();
    }
    
    private void doUnparse(final SqlCall originalNode, final CallTransformer transformer, final SqlWriter writer) {
        final Object[] args = this.getOperatorsAsStringList(originalNode, transformer, writer).toArray();
        writer.print(MessageFormat.format(this.rewrite, args));
    }
}
