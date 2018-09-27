package com.iteblog;

import com.google.common.base.Strings;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by iteblog on 2015-05-29.
 *
 * web: http://www.iteblog.com
 */

public class Spider {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private HttpClient httpClient = null;

    public Spider() {
        httpClient = new HttpClient();
        //设置连接超时时间为10秒（连接初始化时间）
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(10000);
    }

    /**
     * 根据URL创建一个GetMethod，然后设置一些参数返回
     *
     * @param url     需要抓取的网站URL
     * @param timeout 超时
     * @return 返回得到的GetMethod
     */
    private HttpMethodBase createMethod(String url, int timeout) {
        PostMethod method = null;
        try {
            method = new PostMethod(url);
            JSONObject jsonObject = new JSONObject();

            jsonObject.put("blog", "http://www.iteblog.com");
            jsonObject.put("Author", "iteblog");

            String transJson = jsonObject.toString();
            RequestEntity se = new StringRequestEntity(transJson, "application/json", "UTF-8");
            method.setRequestEntity(se);
            //使用系统提供的默认的恢复策略
            method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            //设置超时的时间
            method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);
        } catch (IllegalArgumentException e) {
            logger.error("非法的URL：{}", url);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return method;
    }


    /**
     * 得到一个GetMethod对象，如果存在头部数据，则填充HTTP请求头
     *
     * @param url     需要抓取的网站URL
     * @param timeout 超时
     * @param header  HTTP请求头，可以为空
     * @return 返回一个可能设置好HTTP请求头的GetMethod对象
     */
    private HttpMethodBase getAndPossibleFillGetMethod(String url, int timeout, Map<String, String> header) {
        //logger.info("开始抓取：{}", url);
        HttpMethodBase getMethod = createMethod(url, timeout);
        if (getMethod == null) {
            return null;
        }

        //设置HTTP请求头
        if (header != null) {
            Set<Map.Entry<String, String>> entries = header.entrySet();
            for (Map.Entry<String, String> next : entries) {
                getMethod.setRequestHeader(next.getKey(), next.getValue());
            }

        }
        return getMethod;
    }

    /**
     * 得到网页的源码
     *
     * @param url     请求的URL
     * @param timeout 超时
     * @param header  请求头，可以为空
     * @return 获取到的源码
     */
    public String getHTML(String url, int timeout, Map<String, String> header, String charset) {
        if (Strings.isNullOrEmpty(url)) {
            logger.error("URL路径不能为空!");
            throw new IllegalArgumentException("URL路径为空!");
        }

        if (timeout <= 0) {
            logger.error("超时设置错误({})!", timeout);
            throw new IllegalArgumentException("超时设置错误(" + timeout + ")!");
        }

        if (Strings.isNullOrEmpty(charset)) {
            logger.error("字符集不能为空!");
            throw new IllegalArgumentException("字符集不能为空!");
        }

        StringBuilder builder = null;
        int statusCode;
        try {

            HttpMethodBase getMethod = getAndPossibleFillGetMethod(url, timeout, header);
            if (getMethod == null) {
                return null;
            }
            statusCode = httpClient.executeMethod(getMethod);
            //只要在获取源码中，服务器返回的不是200代码，则统一认为抓取源码失败，返回null。
            if (statusCode != HttpStatus.SC_OK) {
                logger.error("Method failed: " + getMethod.getStatusLine() + "\tstatusCode: " + statusCode);
                return null;
            }

            //下面是用于获取服务器返回的源码数据
            InputStream inStream = getMethod.getResponseBodyAsStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, charset));

            String line = "";
            builder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            //释放连接
            getMethod.releaseConnection();
        } catch (IOException e) {
            logger.error("读取{}出现异常，详细情况：{}", url, e.toString());
            return null;
        }


        return builder.toString();
    }

}
