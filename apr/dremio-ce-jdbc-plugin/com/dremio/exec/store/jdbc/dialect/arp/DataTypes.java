package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;
import com.dremio.common.map.*;
import java.util.*;

class DataTypes
{
    @JsonIgnore
    private final Map<String, Mapping> defaultCastSpecMap;
    @JsonIgnore
    private final Map<String, Mapping> sourceTypeToMappingMap;
    
    DataTypes(@JsonProperty("mappings") final List<Mapping> mappings) {
        final Map<String, Mapping> interimCastMap = (Map<String, Mapping>)Maps.newHashMap();
        for (final Mapping mapping : mappings) {
            if (mapping.isDefaultCastSpec() || !interimCastMap.containsKey(mapping.getDremio().getSqlTypeName())) {
                interimCastMap.put(mapping.getDremio().getSqlTypeName(), mapping);
            }
        }
        this.defaultCastSpecMap = (Map<String, Mapping>)CaseInsensitiveMap.newImmutableMap((Map)interimCastMap);
        final Map<String, Mapping> interimSourceMap = (Map<String, Mapping>)Maps.newHashMap();
        for (final Mapping mapping2 : mappings) {
            final Mapping oldMapping = interimSourceMap.put(mapping2.getSource().getName().toUpperCase(Locale.ROOT), mapping2);
            if (oldMapping != null) {
                throw new IllegalArgumentException(String.format("Duplicate mapping found for source type %s:", mapping2.getSource().getName()));
            }
        }
        this.sourceTypeToMappingMap = (Map<String, Mapping>)CaseInsensitiveMap.newImmutableMap((Map)interimSourceMap);
    }
    
    public Map<String, Mapping> getDefaultCastSpecMap() {
        return this.defaultCastSpecMap;
    }
    
    public Map<String, Mapping> getSourceTypeToMappingMap() {
        return this.sourceTypeToMappingMap;
    }
}
