package com.dremio.exec.store.jdbc;

import com.google.common.collect.*;
import org.apache.arrow.vector.types.*;

public class JdbcTypeConverter
{
    private static final ImmutableMap<Integer, SupportedJdbcType> JDBC_TYPE_MAPPINGS;
    
    public Types.MinorType getMinorType(final int jdbcType) {
        final SupportedJdbcType type = (SupportedJdbcType)JdbcTypeConverter.JDBC_TYPE_MAPPINGS.get((Object)jdbcType);
        if (type == null) {
            return Types.MinorType.VARCHAR;
        }
        return type.minorType;
    }
    
    static {
        JDBC_TYPE_MAPPINGS = ImmutableMap.builder().put((Object)8, (Object)new SupportedJdbcType(8, Types.MinorType.FLOAT8)).put((Object)6, (Object)new SupportedJdbcType(6, Types.MinorType.FLOAT4)).put((Object)(-6), (Object)new SupportedJdbcType(4, Types.MinorType.INT)).put((Object)5, (Object)new SupportedJdbcType(4, Types.MinorType.INT)).put((Object)4, (Object)new SupportedJdbcType(4, Types.MinorType.INT)).put((Object)(-5), (Object)new SupportedJdbcType(-5, Types.MinorType.BIGINT)).put((Object)1, (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).put((Object)12, (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).put((Object)(-1), (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).put((Object)2005, (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).put((Object)(-15), (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).put((Object)(-9), (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).put((Object)(-16), (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).put((Object)(-3), (Object)new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).put((Object)(-2), (Object)new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).put((Object)(-4), (Object)new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).put((Object)2004, (Object)new SupportedJdbcType(-3, Types.MinorType.VARBINARY)).put((Object)2, (Object)new SupportedJdbcType(3, Types.MinorType.DECIMAL)).put((Object)3, (Object)new SupportedJdbcType(3, Types.MinorType.DECIMAL)).put((Object)7, (Object)new SupportedJdbcType(8, Types.MinorType.FLOAT8)).put((Object)91, (Object)new SupportedJdbcType(91, Types.MinorType.DATEMILLI)).put((Object)92, (Object)new SupportedJdbcType(92, Types.MinorType.TIMEMILLI)).put((Object)93, (Object)new SupportedJdbcType(93, Types.MinorType.TIMESTAMPMILLI)).put((Object)16, (Object)new SupportedJdbcType(16, Types.MinorType.BIT)).put((Object)(-7), (Object)new SupportedJdbcType(16, Types.MinorType.BIT)).put((Object)2000, (Object)new SupportedJdbcType(12, Types.MinorType.VARCHAR)).build();
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
