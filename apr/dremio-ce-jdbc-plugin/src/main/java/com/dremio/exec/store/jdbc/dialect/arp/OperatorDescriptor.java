package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.expression.*;
import java.util.*;
import com.dremio.common.dialect.arp.transformer.*;
import org.apache.calcite.rex.*;
import java.util.function.*;
import java.util.stream.*;
import org.apache.calcite.rel.type.*;
import com.dremio.exec.store.jdbc.dialect.*;
import org.slf4j.*;
import com.google.common.collect.*;

public class OperatorDescriptor
{
    private static final Logger logger;
    private static final CompleteType SIMPLE_DECIMAL;
    private final String name;
    private final CompleteType returnType;
    private final List<CompleteType> arguments;
    private final boolean isVarArgs;
    private static final Map<String, String> operatorNameToArpNameMap;
    
    OperatorDescriptor(final String name, final CompleteType returnType, final List<CompleteType> args, final boolean isVarArgs) {
        this.name = name;
        this.returnType = returnType;
        this.arguments = args;
        this.isVarArgs = isVarArgs;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof OperatorDescriptor)) {
            return false;
        }
        final OperatorDescriptor rhs = (OperatorDescriptor)o;
        if (!Objects.equals(rhs.name.toUpperCase(Locale.ROOT), this.name.toUpperCase(Locale.ROOT)) || !Objects.equals(rhs.returnType, this.returnType)) {
            return false;
        }
        if (!this.isVarArgs) {
            return Objects.equals(rhs.arguments, this.arguments);
        }
        return rhs.arguments.isEmpty() || this.arguments.isEmpty() || rhs.arguments.get(0).equals(this.arguments.get(0));
    }
    
    @Override
    public int hashCode() {
        if (!this.isVarArgs) {
            return Objects.hash(this.name.toUpperCase(Locale.ROOT), this.returnType, this.arguments);
        }
        return Objects.hash(this.name.toUpperCase(Locale.ROOT), this.returnType);
    }
    
    public String getName() {
        return this.name;
    }
    
    public CompleteType getReturnType() {
        return this.returnType;
    }
    
    public List<CompleteType> getArguments() {
        return this.arguments;
    }
    
    public boolean isVarArgs() {
        return this.isVarArgs;
    }
    
    static OperatorDescriptor createFromRexCall(final RexCall call, final CallTransformer transformer, final boolean isDistinct, final boolean isVarags) {
        final List<RelDataType> argTypes = (List<RelDataType>)transformer.transformRexOperands(call.operands).stream().map(RexNode::getType).collect(Collectors.toList());
        final RelDataType returnType = call.getType();
        String name = getArpOperatorNameFromOperator(call.getOperator().getName());
        name = transformer.adjustNameBasedOnOperands(name, (List)call.operands);
        OperatorDescriptor.logger.debug("Searching in ARP for {} with types {} and return {}", new Object[] { name, argTypes, returnType });
        return createFromRelTypes(name, isDistinct, returnType, argTypes, isVarags);
    }
    
    static OperatorDescriptor createFromRelTypes(String name, final boolean isDistinct, final RelDataType returnType, final List<RelDataType> argTypes, final boolean isVarArgs) {
        CompleteType returnAsCompleteType = SourceTypeDescriptor.getType(returnType);
        if (returnAsCompleteType.isDecimal()) {
            returnAsCompleteType = OperatorDescriptor.SIMPLE_DECIMAL;
        }
        final List<CompleteType> argsAsCompleteTypes = argTypes.stream().map(SourceTypeDescriptor::getType).map(t -> t.isDecimal() ? OperatorDescriptor.SIMPLE_DECIMAL : t).collect(Collectors.toList());
        if (isDistinct) {
            name += "_distinct";
        }
        return new OperatorDescriptor(name, returnAsCompleteType, argsAsCompleteTypes, isVarArgs);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder().append("Operator name: '").append(this.name).append("'\n").append("Is varargs: ").append(this.isVarArgs).append("\n").append("Argument types: ").append(this.arguments.stream().map(CompleteType::toString).collect(Collectors.joining(", "))).append("\n");
        if (this.returnType != null) {
            sb.append("Return type: ").append(this.returnType).append("\n");
        }
        return sb.toString();
    }
    
    static String getArpOperatorNameFromOperator(final String operatorName) {
        final String mappedName = OperatorDescriptor.operatorNameToArpNameMap.get(operatorName);
        if (mappedName != null) {
            return mappedName;
        }
        return operatorName;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)OperatorDescriptor.class);
        SIMPLE_DECIMAL = CompleteType.fromDecimalPrecisionScale(0, 0);
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        operatorNameToArpNameMap = (Map)builder.put("DATETIME_PLUS", "+").put("DATETIME_MINUS", "-").put("$SUM0", "SUM").build();
    }
}
