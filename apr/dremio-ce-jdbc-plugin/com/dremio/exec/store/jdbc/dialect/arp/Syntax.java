package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.*;
import com.fasterxml.jackson.annotation.*;

public class Syntax
{
    private final DremioSqlDialect.ContainerSupport supportsCatalogs;
    private final DremioSqlDialect.ContainerSupport supportsSchemas;
    private final boolean shouldInjectNumericCastToProject;
    private final boolean shouldInjectApproxNumericCastToProject;
    private final String identifierQuote;
    
    public Syntax(@JsonProperty("identifier_quote") final String identifierQuote, @JsonProperty("supports_catalogs") final Boolean supportsCatalogs, @JsonProperty("supports_schemas") final Boolean supportsSchemas, @JsonProperty("inject_numeric_cast_project") final Boolean shouldInjectNumericCastToProject, @JsonProperty("inject_approx_numeric_cast_project") final Boolean shouldInjectApproxNumericCastToProject) {
        this.identifierQuote = identifierQuote;
        this.supportsCatalogs = ((supportsCatalogs == null) ? DremioSqlDialect.ContainerSupport.AUTO_DETECT : (supportsCatalogs ? DremioSqlDialect.ContainerSupport.SUPPORTED : DremioSqlDialect.ContainerSupport.UNSUPPORTED));
        this.supportsSchemas = ((supportsSchemas == null) ? DremioSqlDialect.ContainerSupport.AUTO_DETECT : (supportsSchemas ? DremioSqlDialect.ContainerSupport.SUPPORTED : DremioSqlDialect.ContainerSupport.UNSUPPORTED));
        this.shouldInjectNumericCastToProject = (shouldInjectNumericCastToProject != null && shouldInjectNumericCastToProject);
        this.shouldInjectApproxNumericCastToProject = (shouldInjectApproxNumericCastToProject != null && shouldInjectApproxNumericCastToProject);
    }
    
    public String getIdentifierQuote() {
        return this.identifierQuote;
    }
    
    public DremioSqlDialect.ContainerSupport supportsCatalogs() {
        return this.supportsCatalogs;
    }
    
    public DremioSqlDialect.ContainerSupport supportsSchemas() {
        return this.supportsSchemas;
    }
    
    public boolean shouldInjectNumericCastToProject() {
        return this.shouldInjectNumericCastToProject;
    }
    
    public boolean shouldInjectApproxNumericCastToProject() {
        return this.shouldInjectApproxNumericCastToProject;
    }
}
