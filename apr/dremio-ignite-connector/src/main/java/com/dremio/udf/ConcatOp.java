/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.udf;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
/**
 * 不定长参数
 * @author WBPC1158
 *
 */
public class ConcatOp {

@FunctionTemplate(
    name = "my_concat",
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
public static class VarCharConcatOp implements SimpleFunction {

  @Inject ArrowBuf buffer;

  @Param public NullableVarCharHolder left;
  @Param public NullableVarCharHolder right;
  @Output public NullableVarCharHolder out;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
	if(left.isSet==0 && right.isSet==0) {
		return;
	}
	else if(left.isSet==0) {
		out.buffer = buffer = right.buffer;
	    out.start = right.start;
	    out.end = right.end;
	}
	else if(right.isSet==0) {
		out.buffer = buffer = left.buffer;
	    out.start = left.start;
	    out.end = left.end;
	}
	else {
	    final int bytesLeftArg = left.end - left.start;
	    final int bytesRightArg = right.end - right.start;
	    final int finalLength = bytesLeftArg + bytesRightArg;
	
	    out.buffer = buffer = buffer.reallocIfNeeded(finalLength);
	    out.start = 0;
	    out.end = finalLength;
	
	    left.buffer.getBytes(left.start, out.buffer, 0, bytesLeftArg);
	    right.buffer.getBytes(right.start, out.buffer, bytesLeftArg, bytesRightArg);
	}
  }
}



@FunctionTemplate(
    name = "least",
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
public static class BigIntLeastOp implements SimpleFunction {

  @Param public NullableBigIntHolder left;
  @Param public NullableBigIntHolder right;
  @Output public NullableBigIntHolder out;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
	if(left.isSet==0 && right.isSet==0) {
		return;
	}
	else if(left.isSet==0) {
		out = right;
	}
	else if(right.isSet==0) {
		out = left;
	}
	else if(left.value<=right.value){
		out = left;
	}
	else {
		out = right;
	}
  }
}



@FunctionTemplate(
    name = "least",
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
public static class Float8LeastOp implements SimpleFunction { 

  @Param public NullableFloat8Holder left;
  @Param public NullableFloat8Holder right;
  @Output public NullableFloat8Holder out;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
	if(left.isSet==0 && right.isSet==0) {
		return;
	}
	else if(left.isSet==0) {
		out = right;
	}
	else if(right.isSet==0) {
		out = left;
	}
	else if(left.value<=right.value){
		out = left;
	}
	else {
		out = right;
	}
  }
}

@FunctionTemplate(
    name = "greatest",
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
public static class BigIntGreatestOp implements SimpleFunction {

 
  @Param public NullableBigIntHolder left;
  @Param public NullableBigIntHolder right;
  @Output public NullableBigIntHolder out;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
	if(left.isSet==0 && right.isSet==0) {
		return;
	}
	else if(left.isSet==0) {
		out = right;
	}
	else if(right.isSet==0) {
		out = left;
	}
	else if(left.value>=right.value){
		out = left;
	}
	else {
		out = right;
	}
  }
}


@FunctionTemplate(
    name = "greatest",
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
public static class Float8GreatestOp implements SimpleFunction {

 
  @Param public NullableFloat8Holder left;
  @Param public NullableFloat8Holder right;
  @Output public NullableFloat8Holder out;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
	if(left.isSet==0 && right.isSet==0) {
		return;
	}
	else if(left.isSet==0) {
		out = right;
	}
	else if(right.isSet==0) {
		out = left;
	}
	else if(left.value>=right.value){
		out = left;
	}
	else {
		out = right;
	}
  }
}

}

