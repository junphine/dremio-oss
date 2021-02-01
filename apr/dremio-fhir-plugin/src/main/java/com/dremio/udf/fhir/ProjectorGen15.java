
package com.dremio.udf.fhir;

import java.util.*;
import com.dremio.exec.expr.fn.impl.StringFunctionUtil;
import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;
import java.util.Map;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.VectorResolver;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.project.ProjectorTemplate;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ValueHolderHelper;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

public class ProjectorGen15
    extends ProjectorTemplate
{

    ArrowBuf work0;
    Map work1;
    VarCharVector vv2;
    NullableVarCharHolder string4;
    NullableVarCharHolder constant5;
    NullableVarCharHolder string6;
    NullableVarCharHolder constant7;
    VarCharVector vv9;

    public ProjectorGen15() {
        __INIT__();
    }

    public void doEval(int inIndex, int outIndex)
        throws SchemaChangeException
    {
        {
            NullableVarCharHolder out3 = new NullableVarCharHolder();
            {
                out3 .isSet = vv2 .isSet((inIndex));
                if (out3 .isSet == 1) {
                    out3 .buffer = vv2 .getDataBuffer();
                    long startEnd = vv2 .getStartEnd((inIndex));
                    out3 .start = ((int) startEnd);
                    out3 .end = ((int)(startEnd >> 32));
                }
            }
            //---- start of eval portion of std_term function. ----//
            NullableVarCharHolder out8 = new NullableVarCharHolder();
            {
                if ((((out3 .isSet*constant5 .isSet)*constant7 .isSet)*constant7 .isSet) == 0) {
                    out8 .isSet = 0;
                } else {
                    final NullableVarCharHolder out = new NullableVarCharHolder();
                    NullableVarCharHolder type = out3;
                    NullableVarCharHolder version = constant5;
                    NullableVarCharHolder name = constant7;
                    NullableVarCharHolder group = constant7;
                    ArrowBuf buffer = work0;
                    Map json = work1;
                     
StandaredFunctions$TermStandard_eval: {
    StringBuffer params = new StringBuffer();

    params.append("name=");
    params.append(StringFunctionHelpers.getStringFromVarCharHolder(name));
    if (group.isSet > 0) {
        params.append("&group=");
        params.append(StringFunctionHelpers.toStringFromUTF8(group.start, group.end, group.buffer));
    }

    String verName = version.isSet > 0 ? StringFunctionHelpers.toStringFromUTF8(version.start, version.end, version.buffer) : "";
    String typeName = StringFunctionHelpers.getStringFromVarCharHolder(type);
    String path = "/standardize/gateway/" + typeName + "/" + verName;

    json = com.dremio.udf.fhir.HttpUtil.jsonGetCall(com.dremio.udf.fhir.HttpUtil.serviceUrl + path, params.toString());

    String msg = json.toString();
    byte[] buf = msg.getBytes();
    int finalLength = buf.length;

    out.buffer = buffer = buffer.reallocIfNeeded(finalLength);
    out.start = 0;
    out.end = finalLength;
    out.buffer.setBytes(0, buf);
}
 
                    work0 = buffer;
                    work1 = json;
                    out.isSet = 1;
                    out8 = out;
                }
            }
            //---- end of eval portion of std_term function. ----//
            if (!(out8 .isSet == 0)) {
                vv9 .setSafe((outIndex), out8 .isSet, out8 .start, out8 .end, out8 .buffer);
            }
        }
    }

    public void doSetup(FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing, com.dremio.sabot.op.project.Projector.ComplexWriterCreator writerCreator)
        throws SchemaChangeException
    {
        {
            work0 = (context).getManagedBuffer();
            vv2 = ((VarCharVector) VectorResolver.simple((incoming), VarCharVector.class, 4));
            string4 = ValueHolderHelper.getNullableVarCharHolder((context).getManagedBuffer(), "gbk");
            constant5 = string4;
            string6 = ValueHolderHelper.getNullableVarCharHolder((context).getManagedBuffer(), "null");
            constant7 = string6;
            /** start SETUP for function std_term **/ 
            {
                NullableVarCharHolder version = constant5;
                NullableVarCharHolder name = constant7;
                NullableVarCharHolder group = constant7;
                ArrowBuf buffer = work0;
                Map json = work1;
                 {}
                work0 = buffer;
                work1 = json;
            }
            /** end SETUP for function std_term **/ 
            vv9 = ((VarCharVector) VectorResolver.simple((outgoing), VarCharVector.class, 0));
        }
    }

    public void __INIT__()
        throws SchemaChangeException
    {
    }

}
