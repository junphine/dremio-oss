/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

public class IgniteSystemOp {

	@FunctionTemplate(names = "ifnull",desc="Returns the value of 'a' if it is not null, otherwise 'b'", 
			scope = FunctionTemplate.FunctionScope.SIMPLE, 
			nulls = FunctionTemplate.NullHandling.INTERNAL)
	public static class IFNull implements SimpleFunction {

		@Param
		NullableVarCharHolder a;
		@Param
		NullableVarCharHolder b;
		@Output
		NullableVarCharHolder out;

		public void setup() {
		}

		public void eval() {
			out.isSet = 1;
			out = a.isSet > 0 ? a : b;
		}
	}

	@FunctionTemplate(names =  "NVL2", desc = "NVL2:-X, 'not null', 'null'", 
			scope = FunctionTemplate.FunctionScope.SIMPLE, 
			nulls = FunctionTemplate.NullHandling.INTERNAL)
	public static class NVL2 implements SimpleFunction {

		@Param
		FieldReader fieldReader;
		@Param
		NullableVarCharHolder a;
		@Param
		NullableVarCharHolder b;
		@Output
		NullableVarCharHolder out;

		public void setup() {
		}

		public void eval() {
			out.isSet = 1;
			out = fieldReader.isSet() ? a : b;
		}
	}

	@FunctionTemplate(names = { "CASEWHEN",	"case_when"}, desc = "CASEWHEN:-X>0, 'not null', 'null'", 
			scope = FunctionTemplate.FunctionScope.SIMPLE, 
			nulls = FunctionTemplate.NullHandling.INTERNAL)
	public static class CASEWHEN implements SimpleFunction {

		@Param
		NullableBitHolder test;
		@Param
		NullableVarCharHolder a;
		@Param
		NullableVarCharHolder b;
		@Output
		NullableVarCharHolder out;

		public void setup() {
		}

		public void eval() {
			out.isSet = 1;
			out = test.isSet > 0 && test.value != 0 ? a : b;
		}
	}
}
