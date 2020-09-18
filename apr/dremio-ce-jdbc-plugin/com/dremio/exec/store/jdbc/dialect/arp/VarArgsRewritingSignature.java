package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;
import com.dremio.common.dialect.arp.transformer.*;
import org.apache.calcite.sql.*;
import java.text.*;
import java.util.stream.*;
import com.google.common.base.*;
import java.util.*;

class VarArgsRewritingSignature extends RewritingSignature
{
    private final VariableRewrite rewrite;
    
    VarArgsRewritingSignature(@JsonProperty("return") final String returnType, @JsonProperty("arg_type") final String argType, @JsonProperty("variable_rewrite") final VariableRewrite rewrite) {
        super(returnType, (List<String>)((argType != null) ? ImmutableList.of((Object)argType) : ImmutableList.of()), (rewrite == null) ? null : getRewrite(rewrite.getRewriteFormat()));
        this.rewrite = rewrite;
    }
    
    @Override
    boolean hasRewrite() {
        return this.rewrite != null;
    }
    
    @Override
    protected List<String> getOperatorsAsStringList(final SqlCall originalNode, final CallTransformer transformer, final SqlWriter writer) {
        List<String> operandsAsList = super.getOperatorsAsStringList(originalNode, transformer, writer);
        if (null != this.rewrite.getRewriteArgument()) {
            operandsAsList = operandsAsList.stream().map(operand -> MessageFormat.format(getRewrite(this.rewrite.getRewriteArgument()), operand)).collect((Collector<? super Object, ?, List<String>>)Collectors.toList());
        }
        final ImmutableList.Builder<String> listBuilder = (ImmutableList.Builder<String>)new ImmutableList.Builder();
        for (final String separator : this.rewrite.getSeparatorSequence()) {
            listBuilder.add((Object)Joiner.on(separator).join((Iterable)operandsAsList));
        }
        return (List<String>)listBuilder.build();
    }
    
    @Override
    OperatorDescriptor toOperatorDescriptor(final String name) {
        return new OperatorDescriptor(name, this.returnType, this.args, true);
    }
    
    private static String getRewrite(final String rewrite) {
        if (rewrite == null) {
            return null;
        }
        return rewrite.replaceAll("\\{separator\\[([\\d+?])\\]}", "{$1}");
    }
}
