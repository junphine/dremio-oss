package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.expression.*;
import com.fasterxml.jackson.annotation.*;
import com.google.common.base.*;
import java.util.*;
import com.google.common.annotations.*;
import com.google.common.collect.*;
import com.dremio.common.map.*;
import java.text.*;

public class Mapping
{
    static final Map<String, CompleteType> ARP_TO_DREMIOTYPE_MAP;
    @JsonIgnore
    private final DataType source;
    @JsonIgnore
    private final CompleteType dremio;
    @JsonIgnore
    private final boolean defaultCastSpec;
    @JsonIgnore
    private final RequiredCastArgs requiredCastArgs;
    
    Mapping(@JsonProperty("source") final DataType source, @JsonProperty("dremio") final DataType dremio, @JsonProperty("default_cast_spec") final boolean defaultCastSpec, @JsonProperty("required_cast_args") final String requiredCastArgs) {
        this.source = source;
        this.defaultCastSpec = defaultCastSpec;
        this.dremio = convertDremioTypeStringToCompleteType(dremio.getName(), (source.getMaxPrecision() == null) ? 0 : ((int)source.getMaxPrecision()), (source.getMaxScale() == null) ? 0 : ((int)source.getMaxScale()));
        if (!Strings.isNullOrEmpty(requiredCastArgs)) {
            this.requiredCastArgs = RequiredCastArgs.valueOf(requiredCastArgs.toUpperCase(Locale.ROOT));
        }
        else {
            this.requiredCastArgs = RequiredCastArgs.NONE;
        }
    }
    
    @VisibleForTesting
    public static CompleteType convertDremioTypeStringToCompleteType(final String dremioTypename) {
        return convertDremioTypeStringToCompleteType(dremioTypename, 0, 0);
    }
    
    @VisibleForTesting
    public static CompleteType convertDremioTypeStringToCompleteType(final String dremioTypename, final int precision, final int scale) {
        final CompleteType dremioType = Mapping.ARP_TO_DREMIOTYPE_MAP.get(dremioTypename);
        if (dremioType == null) {
            throw new RuntimeException(String.format("Invalid Dremio typename specified in ARP file: '%s'.", dremioTypename));
        }
        if (dremioType.isDecimal()) {
            return CompleteType.fromDecimalPrecisionScale(precision, scale);
        }
        return dremioType;
    }
    
    public CompleteType getDremio() {
        return this.dremio;
    }
    
    public DataType getSource() {
        return this.source;
    }
    
    public boolean isDefaultCastSpec() {
        return this.defaultCastSpec;
    }
    
    public RequiredCastArgs getRequiredCastArgs() {
        return this.requiredCastArgs;
    }
    
    static {
        final ImmutableMap<String, CompleteType> interimMap = ImmutableMap.<String, CompleteType>builder()
        		.put("bigint", CompleteType.BIGINT).put("boolean", CompleteType.BIT)
        		.put("date", CompleteType.DATE).put("decimal", CompleteType.DECIMAL)
        		.put("double", CompleteType.DOUBLE).put("float", CompleteType.FLOAT)
        		.put("integer", CompleteType.INT)
        		.put("interval_day_second", CompleteType.INTERVAL_DAY_SECONDS)
        		.put("interval_year_month", CompleteType.INTERVAL_YEAR_MONTHS)
        		.put("time", CompleteType.TIME)
        		.put("timestamp", CompleteType.TIMESTAMP)
        		.put("varbinary", CompleteType.VARBINARY)
        		.put("varchar", CompleteType.VARCHAR).build();
        
        ARP_TO_DREMIOTYPE_MAP = CaseInsensitiveMap.newImmutableMap(interimMap);
    }
    
    enum RequiredCastArgs
    {
        NONE(0) {
            @Override
            public String serializeArguments(final String sourceTypeName, final int precision, final int scale) {
                return MessageFormat.format("{0}", sourceTypeName);
            }
        }, 
        PRECISION(1) {
            @Override
            public String serializeArguments(final String sourceTypeName, final int precision, final int scale) {
                return MessageFormat.format("{0}({1})", sourceTypeName, String.valueOf(precision));
            }
        }, 
        SCALE(2) {
            @Override
            public String serializeArguments(final String sourceTypeName, final int precision, final int scale) {
                return MessageFormat.format("{0}({1})", sourceTypeName, String.valueOf(scale));
            }
        }, 
        PRECISION_SCALE(3) {
            @Override
            public String serializeArguments(final String sourceTypeName, final int precision, final int scale) {
                return MessageFormat.format("{0}({1}, {2})", sourceTypeName, String.valueOf(precision), String.valueOf(scale));
            }
        };
        
    	RequiredCastArgs(int i){
    		
    	}
    	
        static RequiredCastArgs getRequiredArgsBasedOnInputs(final boolean hasPrecision, final boolean hasScale, final RequiredCastArgs userSpecifiedArgs) {
            RequiredCastArgs args;
            if (hasPrecision && hasScale) {
                args = RequiredCastArgs.PRECISION_SCALE;
            }
            else if (hasPrecision) {
                args = RequiredCastArgs.PRECISION;
            }
            else if (hasScale) {
                args = RequiredCastArgs.SCALE;
            }
            else {
                args = RequiredCastArgs.NONE;
            }
            switch (args) {
                case PRECISION_SCALE: {
                    return userSpecifiedArgs;
                }
                case PRECISION: {
                    if (userSpecifiedArgs != RequiredCastArgs.PRECISION_SCALE && userSpecifiedArgs != RequiredCastArgs.SCALE) {
                        return userSpecifiedArgs;
                    }
                    return RequiredCastArgs.PRECISION;
                }
                case SCALE: {
                    if (userSpecifiedArgs != RequiredCastArgs.PRECISION_SCALE && userSpecifiedArgs != RequiredCastArgs.PRECISION) {
                        return userSpecifiedArgs;
                    }
                    return RequiredCastArgs.SCALE;
                }
                case NONE: {
                    return RequiredCastArgs.NONE;
                }
                default: {
                    return RequiredCastArgs.NONE;
                }
            }
        }
        
        public abstract String serializeArguments(final String p0, final int p1, final int p2);
    }
}
