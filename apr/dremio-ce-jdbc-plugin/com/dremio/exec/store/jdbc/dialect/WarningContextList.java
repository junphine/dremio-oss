package com.dremio.exec.store.jdbc.dialect;

import com.google.common.collect.*;
import java.util.*;

public class WarningContextList implements Iterable<WarningContext>
{
    private final List<WarningContext> list;
    
    public WarningContextList() {
        this.list = (List<WarningContext>)Lists.newArrayList();
    }
    
    public WarningContextList add(final String key, final String value) {
        this.list.add(new WarningContext(key, value));
        return this;
    }
    
    @Override
    public Iterator<WarningContext> iterator() {
        return this.list.iterator();
    }
}
