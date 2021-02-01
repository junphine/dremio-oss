package com.dremio.udf;

// Source code generated using FreeMarker template VarCharAggrFunctions1.java




import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.base.Charsets;
import com.google.common.collect.ObjectArrays;

import com.google.common.base.Preconditions;
import io.netty.buffer.*;

import org.apache.commons.lang3.ArrayUtils;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.expr.fn.impl.StringFunctionUtil;
import org.apache.arrow.memory.*;
import com.dremio.exec.proto.SchemaDefProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.exception.*;
import com.dremio.common.expression.FieldReference;
import org.apache.arrow.vector.util.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.*;
import org.apache.arrow.vector.holders.*;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.common.util.DremioStringUtils;
import com.dremio.exec.vector.*;
import com.dremio.exec.vector.complex.*;

import org.apache.arrow.memory.OutOfMemoryException;

import com.sun.codemodel.JType;
import com.sun.codemodel.JCodeModel;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.LocalDateTime;
import org.joda.time.Period;

import com.dremio.exec.vector.accessor.sql.TimePrintMillis;
import javax.inject.Inject;


import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.util.ByteFunctionHelpers;
import org.apache.arrow.vector.holders.*;
import javax.inject.Inject;

import io.netty.buffer.ByteBuf;

@SuppressWarnings("unused")

public class AggLastOp {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggLastOp.class);


@FunctionTemplate(name = "lastvalue", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableVarCharLast implements AggrFunction{

  @Param NullableVarCharHolder in;
   
  @Workspace BigIntHolder nonNullCount;
  @Workspace NullableVarCharHolder tmp;
  @Inject ArrowBuf buf;
  @Output NullableVarCharHolder out;

  public void setup() {    
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
  }

  @Override
  public void add() {
    sout: {
      if (in.isSet == 0) {
        // processing nullable input and the value is null, so don't do anything...
        break sout;
      }
      nonNullCount.value+=1;
      tmp = in;
    } // end of sout block
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0 && tmp!=null) {
      out.isSet = 1;
      
      buf = buf.reallocIfNeeded(tmp.end-tmp.start);
      buf.setBytes(0, tmp.buffer, tmp.start, tmp.end);
      out.start  = 0;
      out.end    = tmp.end-tmp.start;
      out.buffer = buf;
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {   
    tmp = null;
    nonNullCount.value = 0;
  }
}

@FunctionTemplate(name = "lastvalue", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableVarBinaryLast implements AggrFunction{

  @Param NullableVarBinaryHolder in;
  @Workspace NullableVarBinaryHolder tmp; 
  @Workspace BigIntHolder nonNullCount;
  @Inject ArrowBuf buf;
  @Output NullableVarBinaryHolder out;

  public void setup() {
   
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;   
  }

  @Override
  public void add() {
    sout: {
      if (in.isSet == 0) {
        // processing nullable input and the value is null, so don't do anything...
        break sout;
      }
      nonNullCount.value += 1;
      tmp = in;
    } // end of sout block
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      buf = buf.reallocIfNeeded(tmp.end-tmp.start);
      buf.setBytes(0, tmp.buffer, tmp.start, tmp.end);
      out.start  = 0;
      out.end    = tmp.end-tmp.start;
      out.buffer = buf;
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    tmp = null;
    nonNullCount.value = 0;
  }
}
}
