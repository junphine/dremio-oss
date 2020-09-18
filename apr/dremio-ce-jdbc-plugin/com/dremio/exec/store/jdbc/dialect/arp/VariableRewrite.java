package com.dremio.exec.store.jdbc.dialect.arp;

import java.util.*;
import com.fasterxml.jackson.annotation.*;

class VariableRewrite
{
    private final List<String> separatorSequence;
    private final String rewriteArgument;
    private final String rewriteFormat;
    
    VariableRewrite(@JsonProperty("separator_sequence") final List<String> separatorSequence, @JsonProperty("rewrite_argument") final String rewriteArgument, @JsonProperty("rewrite_format") final String rewriteFormat) {
        this.separatorSequence = separatorSequence;
        this.rewriteArgument = rewriteArgument;
        this.rewriteFormat = rewriteFormat;
    }
    
    public List<String> getSeparatorSequence() {
        return this.separatorSequence;
    }
    
    public String getRewriteFormat() {
        return this.rewriteFormat;
    }
    
    public String getRewriteArgument() {
        return this.rewriteArgument;
    }
}
