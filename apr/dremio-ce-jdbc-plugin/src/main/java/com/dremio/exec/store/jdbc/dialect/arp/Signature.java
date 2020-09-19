package com.dremio.exec.store.jdbc.dialect.arp;

import java.util.*;
import com.dremio.common.expression.*;
import java.util.function.*;
import java.util.stream.*;
import com.fasterxml.jackson.annotation.*;
import com.dremio.common.dialect.arp.transformer.*;
import org.apache.calcite.sql.*;

class Signature
{
    protected final List<CompleteType> args;
    protected final CompleteType returnType;
    
    Signature(final String returnType, final List<String> args) {
        this.returnType = Mapping.convertDremioTypeStringToCompleteType(returnType);
        this.args = args.stream().map(Mapping::convertDremioTypeStringToCompleteType).collect(Collectors.toList());
    }
    
    OperatorDescriptor toOperatorDescriptor(final String name) {
        return new OperatorDescriptor(name, this.returnType, this.args, false);
    }
    
    boolean hasRewrite() {
        return false;
    }
    
    @JsonCreator
    static Signature createSignature(@JsonProperty("return") final String returnType, @JsonProperty("args") final List<String> args, @JsonProperty("rewrite") final String rewrite) {
        if (rewrite == null) {
            return new Signature(returnType, args);
        }
        return new RewritingSignature(returnType, args, rewrite);
    }
    
    public void unparse(final SqlCall originalNode, final CallTransformer transformer, final SqlWriter writer, final int leftPrec, final int rightPrec) {
        throw new UnsupportedOperationException("Unparse should only be used with rewriting signatures.");
    }
}
