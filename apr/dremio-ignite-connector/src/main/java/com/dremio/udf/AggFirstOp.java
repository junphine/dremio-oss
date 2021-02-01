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

public class AggFirstOp {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggFirstOp.class);


@FunctionTemplate(name = "firstvalue", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableVarCharFirst implements AggrFunction{

  @Param NullableVarCharHolder in;
  @Workspace ObjectHolder value;
  @Workspace NullableIntHolder init;
  @Workspace BigIntHolder nonNullCount;
  @Inject ArrowBuf buf;
  @Output NullableVarCharHolder out;

  public void setup() {
    init = new NullableIntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    init.value = 0;
    value = new ObjectHolder();
    com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = new com.dremio.exec.expr.fn.impl.ByteArrayWrapper();
    value.obj = tmp;
  }

  @Override
  public void add() {
    sout: {
      if (in.isSet == 0) {
        // processing nullable input and the value is null, so don't do anything...
        break sout;
      }
    nonNullCount.value = 1;
    com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = (com.dremio.exec.expr.fn.impl.ByteArrayWrapper) value.obj;
   
    boolean swap = false;

    // if buffer is null then swap
    if (init.value == 0) {
      init.value = 1;
      swap = true;
    } else { 

      swap = false;
    }
    if (swap) {
      int inputLength = in.end - in.start;
      if (tmp.getLength() >= inputLength) {
        in.buffer.getBytes(in.start, tmp.getBytes(), 0, inputLength);
        tmp.setLength(inputLength);
      } else {
        byte[] tempArray = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
        tmp.setBytes(tempArray);
      }
    }
    } // end of sout block
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = (com.dremio.exec.expr.fn.impl.ByteArrayWrapper) value.obj;
      buf = buf.reallocIfNeeded(tmp.getLength());
      buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
      out.start  = 0;
      out.end    = tmp.getLength();
      out.buffer = buf;
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    value = new ObjectHolder();
    value.obj = new com.dremio.exec.expr.fn.impl.ByteArrayWrapper();
    init.value = 0;
    nonNullCount.value = 0;
  }
}

@FunctionTemplate(name = "firstvalue", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableVarBinaryFirst implements AggrFunction{

  @Param NullableVarBinaryHolder in;
  @Workspace ObjectHolder value;
  @Workspace NullableIntHolder init;
  @Workspace BigIntHolder nonNullCount;
  @Inject ArrowBuf buf;
  @Output NullableVarBinaryHolder out;

  public void setup() {
    init = new NullableIntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    init.value = 0;
    value = new ObjectHolder();
    com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = new com.dremio.exec.expr.fn.impl.ByteArrayWrapper();
    value.obj = tmp;
  }

  @Override
  public void add() {
    sout: {
      if (in.isSet == 0) {
        // processing nullable input and the value is null, so don't do anything...
        break sout;
      }
    nonNullCount.value = 1;
    com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = (com.dremio.exec.expr.fn.impl.ByteArrayWrapper) value.obj;
    
    boolean swap = false;

    // if buffer is null then swap
    if (init.value == 0) {
      init.value = 1;
      swap = true;
    } else {

      swap = false;
    }
    if (swap) {
      int inputLength = in.end - in.start;
      if (tmp.getLength() >= inputLength) {
        in.buffer.getBytes(in.start, tmp.getBytes(), 0, inputLength);
        tmp.setLength(inputLength);
      } else {
        byte[] tempArray = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
        tmp.setBytes(tempArray);
      }
    }
    } // end of sout block
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = (com.dremio.exec.expr.fn.impl.ByteArrayWrapper) value.obj;
      buf = buf.reallocIfNeeded(tmp.getLength());
      buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
      out.start  = 0;
      out.end    = tmp.getLength();
      out.buffer = buf;
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    value = new ObjectHolder();
    value.obj = new com.dremio.exec.expr.fn.impl.ByteArrayWrapper();
    init.value = 0;
    nonNullCount.value = 0;
  }
}
}
