package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import org.apache.calcite.sql.*;

public class JoinSupport extends RelationalAlgebraOperation
{
    private final JoinOp crossJoin;
    private final JoinOpWithCondition innerJoin;
    private final JoinOpWithCondition rightJoin;
    private final JoinOpWithCondition leftJoin;
    private final JoinOpWithCondition fullOuterJoin;
    
    JoinSupport(@JsonProperty("enable") final boolean enable, @JsonProperty("cross") final JoinOp crossJoin, @JsonProperty("inner") final JoinOpWithCondition innerJoin, @JsonProperty("left") final JoinOpWithCondition leftJoin, @JsonProperty("right") final JoinOpWithCondition rightJoin, @JsonProperty("full") final JoinOpWithCondition fullOuterJoin) {
        super(enable);
        this.crossJoin = crossJoin;
        this.innerJoin = innerJoin;
        this.rightJoin = rightJoin;
        this.leftJoin = leftJoin;
        this.fullOuterJoin = fullOuterJoin;
    }
    
    public JoinOp getJoinOp(final JoinType type) {
        switch (type) {
            case COMMA:
            case CROSS: {
                return this.crossJoin;
            }
            case INNER: {
                return this.innerJoin;
            }
            case LEFT: {
                return this.leftJoin;
            }
            case RIGHT: {
                return this.rightJoin;
            }
            default: {
                return this.fullOuterJoin;
            }
        }
    }
}
