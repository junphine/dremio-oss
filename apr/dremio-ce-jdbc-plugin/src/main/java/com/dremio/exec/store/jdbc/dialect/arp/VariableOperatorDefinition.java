package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;
import java.util.*;

public class VariableOperatorDefinition extends OperatorDefinition
{
    VariableOperatorDefinition(@JsonProperty("names") final List<String> names, @JsonProperty("variable_signatures") final List<VarArgsRewritingSignature> signatures) {
        super(names, (List<Signature>)ImmutableList.copyOf((Collection)signatures));
    }
}
