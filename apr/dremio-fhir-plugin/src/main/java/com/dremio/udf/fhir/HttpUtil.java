package com.dremio.udf.fhir;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

public class HttpUtil {
	
 public static String serviceUrl = "http://127.0.0.1:8888/disease";
	 
 public static Map<String,Object> jsonGetCall(String path,String params) {
	    
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();

		
		// 创建Get请求
		HttpGet httpGet = new HttpGet(path +"?"+ params);
		// 响应模型
		CloseableHttpResponse response = null;
		try {
			// 配置信息
			RequestConfig requestConfig = RequestConfig.custom()
					// 设置连接超时时间(单位毫秒)
					.setConnectTimeout(5000)
					// 设置请求超时时间(单位毫秒)
					.setConnectionRequestTimeout(600000)
					// socket读写超时时间(单位毫秒)
					.setSocketTimeout(600000)
					// 设置是否允许重定向(默认为true)
					.setRedirectsEnabled(true).build();

			// 将上面的配置信息 运用到这个Get请求里
			httpGet.setConfig(requestConfig);

			// 由客户端执行(发送)Get请求
			response = httpClient.execute(httpGet);

			// 从响应模型中获取响应实体
			HttpEntity responseEntity = response.getEntity();
			System.out.println("响应状态为:" + response.getStatusLine());
			if (responseEntity != null) {
				
				String content = EntityUtils.toString(responseEntity);
				
				JSONObject json =  new JSONObject(content);
				
				System.out.println("响应内容长度为:" + responseEntity.getContentLength());
				
				System.out.println("响应内容为:" + content);
				
				return json.toMap();
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				// 释放资源
				if (httpClient != null) {
					httpClient.close();
				}
				if (response != null) {
					response.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	     
 }
}
