package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.conf.*;
import com.google.common.base.*;
import com.dremio.exec.store.jdbc.dialect.arp.*;
import com.google.common.annotations.*;

public abstract class LegacyCapableJdbcConf<T extends AbstractArpConf<T>> extends AbstractArpConf<T>
{
    @Override
    public JdbcDremioSqlDialect getDialect() {
        final ArpDialect arpDialect = this.getArpDialect();
        final LegacyDialect legacyDialect = this.getLegacyDialect();
        Preconditions.checkArgument(arpDialect.getDatabaseProduct() == legacyDialect.getDatabaseProduct(), "Legacy dialect and ARP dialect should be for the same Database product.");
        if (!this.getLegacyFlag()) {
            return arpDialect;
        }
        return legacyDialect;
    }
    
    protected abstract ArpDialect getArpDialect();
    
    protected abstract LegacyDialect getLegacyDialect();
    
    protected abstract boolean getLegacyFlag();
    
    @VisibleForTesting
    public boolean supportsExternalQuery(final boolean isExternalQueryEnabled) {
        return !this.getLegacyFlag() && isExternalQueryEnabled;
    }
}
