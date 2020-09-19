package com.dremio.exec.store.jdbc;

import com.google.common.collect.*;
import org.apache.arrow.vector.types.*;

public class JdbcTypeConverter
{
    private static final ImmutableMap<Object, Object> JDBC_TYPE_MAPPINGS;
    
    public Types.MinorType getMinorType(final int jdbcType) {
        final SupportedJdbcType type = (SupportedJdbcType)JdbcTypeConverter.JDBC_TYPE_MAPPINGS.get(jdbcType);
        if (type == null) {
            return Types.MinorType.VARCHAR;
        }
        return type.minorType;
    }
    
    static {
        JDBC_TYPE_MAPPINGS = ImmutableMap.builder().
        		put(8, new SupportedJdbcType(8, Types.MinorType.FLOAT8)).
        		put(6, new SupportedJdbcType(6, Types.MinorType.FLOAT4)).
        		put(-6, new SupportedJdbcType(4, Types.MinorType.INT)).
        		put(5, new SupportedJdbcType(4, Types.MinorType.INT)).
        		put(4, new SupportedJdbcType(4, Types.MinorType.INT)).
        		put(-5, new SupportedJdbcType(-5, Types.MinorType.BIGINT)).
        		put(1, new SupportedJdbcType(12, Types.MinorType.VARCHAR)).
        		put(12, new SupportedJdbcType(12, Types.MinorType.VARCHAR)).
        		put(-1, new SupportedJdbcType(12, Types.MinorType.VARCHAR)).
        		put(2005, new SupportedJdbcType(12, Types.MinorType.VARCHAR)).
        		put(-15, new SupportedJdbcType(12, Types.MinorType.VARCHAR)).
        		put((-9), new SupportedJdbcType(12, Types.MinorType.VARCHAR)).
        		put((-16), new SupportedJdbcType(12, Types.MinorType.VARCHAR)).
        		put((-3), new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).
        		put((-2), new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).
        		put((-4), new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).
        		put(2004, new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).
        		put(2, new SupportedJdbcType(3, Types.MinorType.DECIMAL)).
        		put(3, new SupportedJdbcType(3, Types.MinorType.DECIMAL)).
        		put(7, new SupportedJdbcType(8, Types.MinorType.FLOAT8)).
        		put(91, new SupportedJdbcType(91, Types.MinorType.DATEMILLI)).
        		put(92, new SupportedJdbcType(92, Types.MinorType.TIMEMILLI)).
        		put(93, new SupportedJdbcType(93, Types.MinorType.TIMESTAMPMILLI)).
        		put(16, new SupportedJdbcType(16, Types.MinorType.BIT)).
        		put((-7), new SupportedJdbcType(16, Types.MinorType.BIT)).
        		put(2000, new SupportedJdbcType(12, Types.MinorType.VARCHAR)).build();
    }
    
    private static class SupportedJdbcType
    {
        private final int jdbcType;
        private final Types.MinorType minorType;
        
        public SupportedJdbcType(final int jdbcType, final Types.MinorType minorType) {
            this.jdbcType = jdbcType;
            this.minorType = minorType;
        }
    }
    
    public static class DecimalToDoubleJdbcTypeConverter extends JdbcTypeConverter
    {
        @Override
        public Types.MinorType getMinorType(final int jdbcType) {
            if (3 == jdbcType || 2 == jdbcType) {
                return super.getMinorType(8);
            }
            return super.getMinorType(jdbcType);
        }
    }
}
