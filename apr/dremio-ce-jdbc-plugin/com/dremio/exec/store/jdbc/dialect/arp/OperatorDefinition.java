package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;
import java.util.*;

class OperatorDefinition
{
    private final List<String> names;
    private final List<Signature> signatures;
    
    OperatorDefinition(@JsonProperty("names") final List<String> names, @JsonProperty("signatures") final List<Signature> signatures) {
        this.names = names;
        this.signatures = signatures;
    }
    
    List<String> getNames() {
        return this.names;
    }
    
    List<Signature> getSignatures() {
        return this.signatures;
    }
    
    public static Map<OperatorDescriptor, Signature> buildOperatorMap(final List<? extends OperatorDefinition> operators) {
        final ImmutableMap.Builder<OperatorDescriptor, Signature> builder = (ImmutableMap.Builder<OperatorDescriptor, Signature>)ImmutableMap.builder();
        for (final OperatorDefinition op : operators) {
            for (final String name : op.getNames()) {
                for (final Signature sig : op.getSignatures()) {
                    builder.put((Object)sig.toOperatorDescriptor(name), (Object)sig);
                }
            }
        }
        try {
            return (Map<OperatorDescriptor, Signature>)builder.build();
        }
        catch (IllegalArgumentException e) {
            final Set<OperatorDescriptor> debuggingSet = (Set<OperatorDescriptor>)Sets.newHashSet();
            for (final OperatorDefinition op2 : operators) {
                for (final String name2 : op2.getNames()) {
                    for (final Signature sig2 : op2.getSignatures()) {
                        final OperatorDescriptor desc = sig2.toOperatorDescriptor(name2);
                        if (!debuggingSet.add(desc)) {
                            throw new IllegalArgumentException(String.format("Duplicate operator definition: %s", desc), e);
                        }
                    }
                }
            }
            throw e;
        }
    }
}
