package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.server.*;
import javax.inject.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.*;

public abstract class JdbcConf<T extends DialectConf<T, JdbcStoragePlugin>> extends DialectConf<T, JdbcStoragePlugin>
{
    protected static final String ENABLE_EXTERNAL_QUERY_LABEL = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)";
    
    protected abstract JdbcStoragePlugin.Config toPluginConfig(final SabotContext p0);
    
    public JdbcStoragePlugin newPlugin(final SabotContext context, final String name, final Provider<StoragePluginId> pluginIdProvider) {
        return new JdbcStoragePlugin(this.toPluginConfig(context), context, name, pluginIdProvider);
    }
}
