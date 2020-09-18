package com.dremio.exec.store.jdbc.dialect.arp;

import com.google.common.collect.*;
import com.dremio.exec.store.jdbc.dialect.*;
import com.dremio.exec.work.foreman.*;
import java.util.*;
import com.dremio.common.expression.*;

public class ArpTypeMapper extends TypeMapper
{
    private final ArpYaml yaml;
    
    protected ArpTypeMapper(final ArpYaml yaml) {
        super(false);
        this.yaml = yaml;
    }
    
    private void mapSkippedColumnAsNullField(final boolean mapSkippedColumnAsNullType, final SourceTypeDescriptor column, final ImmutableList.Builder<JdbcToFieldMapping> builder) {
        if (mapSkippedColumnAsNullType) {
            builder.add((Object)JdbcToFieldMapping.createSkippedField(column.getColumnIndex(), column));
        }
    }
    
    @Override
    protected List<JdbcToFieldMapping> mapSourceToArrowFields(final UnrecognizedTypeCallback unrecognizedMappingCallback, final AddPropertyCallback addColumnPropertyCallback, final List<SourceTypeDescriptor> columnInfo, final boolean mapSkippedColumnsAsNullType) {
        final ImmutableList.Builder<JdbcToFieldMapping> builder = (ImmutableList.Builder<JdbcToFieldMapping>)ImmutableList.builder();
        for (final SourceTypeDescriptor column : columnInfo) {
            try {
                if (this.shouldIgnore(column)) {
                    unrecognizedMappingCallback.mark(column, true);
                    this.mapSkippedColumnAsNullField(mapSkippedColumnsAsNullType, column, builder);
                }
                else {
                    final Mapping mapping = this.yaml.getMapping(column);
                    CompleteType type;
                    if (mapping == null) {
                        type = unrecognizedMappingCallback.mark(column, false);
                    }
                    else {
                        type = mapping.getDremio();
                    }
                    final JdbcToFieldMapping newMapping = new JdbcToFieldMapping(type, column.getColumnIndex(), column);
                    if (addColumnPropertyCallback != null) {
                        addColumnPropertyCallback.addProperty(newMapping.getField().getName(), "sourceTypeName", newMapping.getSourceTypeDescriptor().getDataSourceTypeName());
                    }
                    builder.add((Object)newMapping);
                }
            }
            catch (UnsupportedDataTypeException e) {
                unrecognizedMappingCallback.mark(column, true);
                this.mapSkippedColumnAsNullField(mapSkippedColumnsAsNullType, column, builder);
            }
        }
        return (List<JdbcToFieldMapping>)builder.build();
    }
    
    protected boolean shouldIgnore(final SourceTypeDescriptor column) {
        return false;
    }
}
