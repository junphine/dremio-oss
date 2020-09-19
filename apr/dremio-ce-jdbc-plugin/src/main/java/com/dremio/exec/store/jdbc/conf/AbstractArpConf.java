package com.dremio.exec.store.jdbc.conf;

import java.util.function.*;
import com.dremio.exec.store.jdbc.dialect.arp.*;
import org.slf4j.*;

public abstract class AbstractArpConf<T extends JdbcConf<T>> extends JdbcConf<T>
{
    private static final Logger logger;
    
    protected static <T extends ArpDialect> T loadArpFile(final String pathToArpFile, final Function<ArpYaml, T> dialectConstructor) {
        T dialect;
        try {
            final ArpYaml yaml = ArpYaml.createFromFile(pathToArpFile);
            dialect = dialectConstructor.apply(yaml);
        }
        catch (Exception e) {
            dialect = null;
            AbstractArpConf.logger.error("Error creating dialect from ARP file {}.", pathToArpFile, e);
        }
        return dialect;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)AbstractArpConf.class);
    }
}
