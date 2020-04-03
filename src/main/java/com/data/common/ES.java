package com.data.common;

import net.sf.json.JSONObject;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ES {

    private static final Logger logger = LoggerFactory.getLogger(ES.class);

    public String executePost(String url, Map<String, Object> params) {
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost();
        JSONObject response = null;
        JSONObject json = JSONObject.fromObject(params);
        try{
            StringEntity s = new StringEntity(json.toString());
            s.setContentEncoding("UTF-8");
            s.setContentType("applicatioin/json");
            post.setEntity(s);
            HttpResponse res = client.execute(post);
            if(res.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
                HttpEntity entity = res.getEntity();
                String result = EntityUtils.toString(res.getEntity());
                response = JSONObject.fromObject(result);
            }
        }catch(Exception e) {
           e.printStackTrace();
        }
        return response.toString();
    }


    public String executeGet(String url, Map<String, Object> params) {
        long start = System.currentTimeMillis();
        String apiUrl = url;
        StringBuffer param = new StringBuffer();
        int i = 0;
        for (String key : params.keySet()) {
            if(i == 0){
                param.append("?");
            }else{
                param.append("&");
            }
            param.append(key).append("=").append(params.get(key));
            i++;
        }
        apiUrl += param;
        String result = null;
        HttpClient httpClient = new DefaultHttpClient();
        try{
            HttpGet httpGet = new HttpGet(apiUrl);
            HttpResponse response = httpClient.execute(httpGet);
            int stateCode = response.getStatusLine().getStatusCode();
            logger.info("状态码："+stateCode);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);
        }catch(Exception e) {
           e.printStackTrace();
        }
        logger.debug(result + "///" + (System.currentTimeMillis() - start));
        return result;
    }



    public String httpPostWithJSON(String url, String json) throws IOException {
        String result = "";
        String encoderJson = json;

        org.apache.commons.httpclient.HttpClient httpClient= new org.apache.commons.httpclient.HttpClient();
        PostMethod method = new PostMethod();
        RequestEntity requestEntity = new StringRequestEntity(encoderJson);
        method.setRequestEntity(requestEntity);
        method.addRequestHeader("Content-Type", "application/json; charset=utf-8");
        int code = httpClient.executeMethod(method);
        logger.info("code:"+code);

        result = method.getResponseBodyAsString();
        method.releaseConnection();
        return result;



    }
}
