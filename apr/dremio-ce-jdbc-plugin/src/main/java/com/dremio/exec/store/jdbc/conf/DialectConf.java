package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.store.jdbc.legacy.*;

public abstract class DialectConf<T extends DialectConf<T, P>, P extends StoragePlugin> extends ConnectionConf<T, P>
{
    public abstract JdbcDremioSqlDialect getDialect();
}
