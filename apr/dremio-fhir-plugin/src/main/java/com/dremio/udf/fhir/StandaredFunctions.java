package com.dremio.udf.fhir;

import java.util.Map;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;
import com.dremio.exec.expr.fn.impl.SubtractFunctions;
import com.dremio.udf.fhir.HttpUtil;

/**
 *   数据标化接口
 * @author WBPC1158
 *
 */
public class StandaredFunctions {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SubtractFunctions.class);
  
  
  @FunctionTemplate(name = "std_term", scope = FunctionScope.SIMPLE, costCategory=FunctionCostCategory.COMPLEX, nulls = NullHandling.NULL_IF_NULL)
  public static class TermStandard implements SimpleFunction {

	@Inject ArrowBuf buffer;
    
    @Param VarCharHolder type;
    @Param NullableVarCharHolder version;
    @Param VarCharHolder name;
    @Param NullableVarCharHolder group;
    @Output VarCharHolder out;

    public void setup() {
    }

    public void eval() {
    	// 参数
		StringBuffer params = new StringBuffer();
	
		// 字符数据最好encoding以下;这样一来，某些特殊字符才能传过去(如:某人的名字就是“&”,不encoding的话,传不过去)
				
		params.append("name=");
		params.append(StringFunctionHelpers.getStringFromVarCharHolder(name));
		
		if(group.isSet>0) {
			params.append("&group=");
			params.append(StringFunctionHelpers.toStringFromUTF8(group.start,group.end,group.buffer));			
		}
	
		String verName = version.isSet>0? StringFunctionHelpers.toStringFromUTF8(version.start,version.end,version.buffer):"";
		String typeName = StringFunctionHelpers.getStringFromVarCharHolder(type);
		
		String path = "/standardize/gateway/"+typeName+"/"+verName;
		
		Map<String,Object> json = com.dremio.udf.fhir.HttpUtil.jsonGetCall(com.dremio.udf.fhir.HttpUtil.serviceUrl + path,params.toString());
		
		String msg = json.toString();
		byte[] buf = msg.getBytes();
    	int finalLength = buf.length;
    	out.buffer = buffer = buffer.reallocIfNeeded(finalLength);
	    out.start = 0;
	    out.end = finalLength;
	    out.buffer.setBytes(0, buf);
	    
    }
  }
  
  
  @FunctionTemplate(name = "std_process_table", scope = FunctionScope.SIMPLE, costCategory=FunctionCostCategory.COMPLEX,nulls = NullHandling.NULL_IF_NULL)
  public static class TermStandardProcessTable implements SimpleFunction {

	@Inject ArrowBuf buffer;	  
    
    @Param VarCharHolder type;
    @Param NullableVarCharHolder version;
    @Param VarCharHolder table;
    @Output VarCharHolder out;

    public void setup() {
    }

    public void eval() {
    	
		// 参数
		StringBuffer params = new StringBuffer();
	
		// 字符数据最好encoding以下;这样一来，某些特殊字符才能传过去(如:某人的名字就是“&”,不encoding的话,传不过去)
		params.append("withType=false");		
		params.append("&save=true");		
		params.append("&tableName=");
		params.append(StringFunctionHelpers.getStringFromVarCharHolder(table));
	
		String verName = version.isSet>0? StringFunctionHelpers.toStringFromUTF8(version.start,version.end,version.buffer):"";
		String typeName = StringFunctionHelpers.getStringFromVarCharHolder(type);
		
		String path = "/standardize/processTable/"+typeName+"/"+verName;
		
		com.dremio.udf.fhir.HttpUtil.jsonGetCall(com.dremio.udf.fhir.HttpUtil.serviceUrl +path,params.toString());
		
		String msg = "OK";//json.toString();
		byte[] buf = msg.getBytes();
    	int finalLength = buf.length;
    	out.buffer = buffer = buffer.reallocIfNeeded(finalLength);
	    out.start = 0;
	    out.end = finalLength;
	    out.buffer.setBytes(0, buf);
    }
  }
  
  

  @FunctionTemplate(name = "std_service_option", 
		  desc = "设置标化服务器的服务url，option name:url",
		  scope = FunctionScope.SIMPLE, 
		  nulls = NullHandling.NULL_IF_NULL)
  public static class TermStandardServiceOption implements SimpleFunction {

	@Inject ArrowBuf buffer;
	  
    @Param VarCharHolder name;
    @Param VarCharHolder value;
   
    @Output VarCharHolder out;

    public void setup() {
    }

    public void eval() {
    	String msg = "failed!not found option name.";
    	String optName = StringFunctionHelpers.getStringFromVarCharHolder(name);
    	if(optName.equalsIgnoreCase("url")) {
    		com.dremio.udf.fhir.HttpUtil.serviceUrl = StringFunctionHelpers.getStringFromVarCharHolder(value);
    		msg = "OK.";
    	}
    	else {
    		
    	}
    	byte[] buf = msg.getBytes();
    	int finalLength = buf.length;
    	out.buffer = buffer = buffer.reallocIfNeeded(finalLength);
	    out.start = 0;
	    out.end = finalLength;
	    out.buffer.setBytes(0, buf);
	    
    }
  }

}
