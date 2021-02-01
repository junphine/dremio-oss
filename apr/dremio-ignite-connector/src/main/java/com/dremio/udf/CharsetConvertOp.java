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

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;
import com.dremio.udf.*;

public class CharsetConvertOp {

	@FunctionTemplate(
			names = "convert", desc = "convert:- str,src_encoding,dest_encoding", 
			scope = FunctionTemplate.FunctionScope.SIMPLE, 
			nulls = FunctionTemplate.NullHandling.INTERNAL)
	public static class Convert implements SimpleFunction {

		@Inject
		ArrowBuf buffer;

		@Param
		NullableVarCharHolder text;
		@Param
		VarCharHolder srcEncoding;
		@Param
		VarCharHolder destEncoding;
		@Output
		NullableVarCharHolder out;
		
		@Workspace
		CharsetConvertOp _tmp;

		public void setup() {
		}

		public void eval() {
			out = text;
			if (text.isSet == 1) {
				String src = CharsetConvertOp.toStringFromEncoding(text.start, text.end, text.buffer,
						StringFunctionHelpers.getStringFromVarCharHolder(srcEncoding));
				out.buffer = buffer;
				out.start = 0;
				out.end= CharsetConvertOp.copyBytes(src, StringFunctionHelpers.getStringFromVarCharHolder(destEncoding), buffer);
			}

		}
	}


	@FunctionTemplate(
			names = "convertToUtf8", desc = "convertToUtf8 from src_encoding" , 
			scope = FunctionTemplate.FunctionScope.SIMPLE, 
			nulls = FunctionTemplate.NullHandling.INTERNAL)
	public static class ConvertToUtf8 implements SimpleFunction {

		@Inject
		ArrowBuf buffer;

		@Param
		NullableVarCharHolder text;
		@Param
		VarCharHolder srcEncoding;
		
		@Output
		NullableVarCharHolder out;
		
		@Workspace
		CharsetConvertOp _tmp;

		public void setup() {
		}

		public void eval() {
			out = text;
			if (text.isSet == 1) {
				String src = CharsetConvertOp.toStringFromEncoding(text.start, text.end, text.buffer,
						StringFunctionHelpers.getStringFromVarCharHolder(srcEncoding));
				out.buffer = buffer;
				out.start = 0;
				byte[] bytes = src.getBytes();
				buffer.setBytes(0, bytes);
				out.end= bytes.length;
			}

		}
	}
	


	@FunctionTemplate(
			names = "convertFromUtf8", desc = "convertFromUtf8 to dest_encoding" , 
			scope = FunctionTemplate.FunctionScope.SIMPLE, 
			nulls = FunctionTemplate.NullHandling.INTERNAL)
	public static class ConvertFromUtf8 implements SimpleFunction {

		@Inject
		ArrowBuf buffer;

		@Param
		NullableVarCharHolder text;
		@Param
		VarCharHolder destEncoding;
		
		@Output
		NullableVarCharHolder out;
		
		@Workspace
		CharsetConvertOp _tmp;

		public void setup() {
		}

		public void eval() {
			out = text;
			if (text.isSet == 1) {
				String src = CharsetConvertOp.toStringFromEncoding(text.start, text.end, text.buffer,"UTF-8");
				out.buffer = buffer;
				out.start = 0;				
				out.end= CharsetConvertOp.copyBytes(src, StringFunctionHelpers.getStringFromVarCharHolder(destEncoding), buffer);
			}

		}
	}
	
	
	public static String toStringFromEncoding(int start, int end, ArrowBuf buffer, String encoding) {
		byte[] buf = new byte[end - start];
		buffer.getBytes(start, buf, 0, end - start);
		return new String(buf, Charset.forName(encoding));
	}

	public static int copyBytes(String src, String destEncoding, ArrowBuf out) {
		try {
			byte[] destBytes = src.getBytes(Charset.forName(destEncoding));
			out.setBytes(0, destBytes);
			return destBytes.length;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

}
