package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.arp.transformer.*;
import org.apache.calcite.sql.*;

class ArpSqlCall extends SqlBasicCall
{
    private final Signature signature;
    private final SqlCall originalNode;
    private final CallTransformer transformer;
    
    public ArpSqlCall(final Signature signature, final SqlCall originalNode, final CallTransformer transformer) {
        super(originalNode.getOperator(), (SqlNode[])originalNode.getOperandList().toArray(new SqlNode[0]), originalNode.getParserPosition());
        this.signature = signature;
        this.originalNode = originalNode;
        this.transformer = transformer;
    }
    
    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
        this.signature.unparse(this.originalNode, this.transformer, writer, leftPrec, rightPrec);
    }
    
    public String toString() {
        return String.format("Arp SqlCall with signature %s", this.signature.toString());
    }
}
