package com.dremio.exec.store.jdbc.dialect.arp;

import java.util.*;
import com.fasterxml.jackson.annotation.*;

class Expressions
{
    private final Map<OperatorDescriptor, Signature> operators;
    private final Map<OperatorDescriptor, Signature> variableOperators;
    private final SubQuerySupport subQuerySupport;
    private final DateTimeFormatSupport dateTimeFormatSupport;
    
    Expressions(@JsonProperty("operators") final List<OperatorDefinition> operators, @JsonProperty("variable_length_operators") final List<VariableOperatorDefinition> variableOperators, @JsonProperty("subqueries") final SubQuerySupport subQuerySupport, @JsonProperty("datetime_formats") final DateTimeFormatSupport dateTimeFormatSupport) {
        this.operators = OperatorDefinition.buildOperatorMap(operators);
        this.variableOperators = OperatorDefinition.buildOperatorMap(variableOperators);
        this.subQuerySupport = subQuerySupport;
        this.dateTimeFormatSupport = dateTimeFormatSupport;
    }
    
    DateTimeFormatSupport getDateTimeFormatSupport() {
        return this.dateTimeFormatSupport;
    }
    
    Map<OperatorDescriptor, Signature> getOperators() {
        return this.operators;
    }
    
    Map<OperatorDescriptor, Signature> getVariableOperators() {
        return this.variableOperators;
    }
    
    SubQuerySupport getSubQuerySupport() {
        return this.subQuerySupport;
    }
}
