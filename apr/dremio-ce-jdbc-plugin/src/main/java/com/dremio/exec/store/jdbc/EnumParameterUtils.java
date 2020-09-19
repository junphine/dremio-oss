package com.dremio.exec.store.jdbc;

import org.apache.calcite.rex.*;
import org.apache.calcite.avatica.util.*;
import java.util.*;
import com.google.common.base.*;
import org.apache.calcite.sql.*;
import com.google.common.collect.*;

public final class EnumParameterUtils
{
    public static final Map<String, TimeUnitRange> TIME_UNIT_MAPPING;
    
    public static boolean hasTimeUnitAsFirstParam(final List<RexNode> operands) {
        if (isFirstParamAFlag(operands)) {
            final RexLiteral firstAsLiteral = (RexLiteral)operands.get(0);
            return firstAsLiteral.getValue() instanceof TimeUnit || firstAsLiteral.getValue() instanceof TimeUnitRange;
        }
        if (operands.isEmpty() || !(operands.get(0) instanceof RexLiteral)) {
            return false;
        }
        final RexLiteral firstAsLiteral = (RexLiteral)operands.get(0);
        final String unit = (String)firstAsLiteral.getValueAs((Class)String.class);
        return unit != null && EnumParameterUtils.TIME_UNIT_MAPPING.containsKey(unit.toLowerCase(Locale.ROOT));
    }
    
    public static TimeUnitRange getFirstParamAsTimeUnitRange(final List<RexNode> operands) {
        Preconditions.checkArgument(hasTimeUnitAsFirstParam(operands));
        final RexLiteral firstAsLiteral = (RexLiteral)operands.get(0);
        if (firstAsLiteral.getValue() instanceof TimeUnitRange) {
            return (TimeUnitRange)firstAsLiteral.getValue();
        }
        if (firstAsLiteral.getValue() instanceof TimeUnit) {
            final TimeUnitRange range = TimeUnitRange.of((TimeUnit)firstAsLiteral.getValueAs((Class)TimeUnit.class), (TimeUnit)null);
            Preconditions.checkNotNull(range, "Time unit range must be constructed correctly.");
            return range;
        }
        final String unit = (String)firstAsLiteral.getValueAs((Class)String.class);
        Preconditions.checkNotNull(unit, "Time unit range must be constructed correctly.");
        return EnumParameterUtils.TIME_UNIT_MAPPING.get(unit.toLowerCase(Locale.ROOT));
    }
    
    public static boolean isFirstParamAFlag(final List<RexNode> operands) {
        if (operands.isEmpty()) {
            return false;
        }
        final RexNode firstOperand = operands.get(0);
        if (firstOperand.getKind() != SqlKind.LITERAL) {
            return false;
        }
        final RexLiteral firstAsLiteral = (RexLiteral)firstOperand;
        return firstAsLiteral.getTypeName().isSpecial();
    }
    
    static {
        final ImmutableMap.Builder<String, TimeUnitRange> timeUnitBuilder = ImmutableMap.builder();
        TIME_UNIT_MAPPING = (Map)timeUnitBuilder.put("day", TimeUnitRange.DAY).put("hour", TimeUnitRange.HOUR).put("minute", TimeUnitRange.MINUTE).put("second", TimeUnitRange.SECOND).put("week", TimeUnitRange.WEEK).put("year", TimeUnitRange.YEAR).put("month", TimeUnitRange.MONTH).put("quarter", TimeUnitRange.QUARTER).put("decade", TimeUnitRange.DECADE).put("century", TimeUnitRange.CENTURY).put("millennium", TimeUnitRange.MILLENNIUM).build();
    }
}
