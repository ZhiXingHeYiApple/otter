package com.alibaba.otter.manager.biz.common.alarm;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class SendMessage {

    protected static Logger logger = LoggerFactory.getLogger(SendMessage.class);

    /**编码格式。发送编码格式统一用UTF-8*/
    protected final static String ENCODING = "UTF-8";

    /**
     * 公共发送短信信息方法
     * @param content 短信内容
     * @param phone 手机号
     * @return 短信发送是否成功（true OR false）
     */
    public abstract boolean sendSMSNotify(String content, String phone);

    /**
     * 处理返回的信息,每家服务商返回的信息都需要不同的处理
     * @param responseText 短信发送结果
     * @return 处理结果
     */
    public abstract boolean dealMessageStatus(String responseText);


    /**
     * HTTPClient 公共发送方法
     * @param url http地址
     * @param params 参数
     * @return 发送结果
     */
    public String sendMessage(String url, Map<String, String> params) {

        // 创建一个HTTP Client链接
        CloseableHttpClient client = HttpClients.createDefault();

        CloseableHttpResponse response = null;
        try {
            // 构建服务商的URL
            HttpPost method = new HttpPost(url);
            List<NameValuePair> paramList = new ArrayList<NameValuePair>();

            //放入请求参数
            for (Map.Entry<String, String> param : params.entrySet()) {
                NameValuePair pair = new BasicNameValuePair(param.getKey(), param.getValue());
                paramList.add(pair);
            }
            method.setEntity(new UrlEncodedFormEntity(paramList, ENCODING));

            //发送请求
            response = client.execute(method);
            String responseText = null;

            //得到响应信息
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                responseText = EntityUtils.toString(entity);

                // 记录发送信息情况
                logger.info(responseText);
            }
            //返回服务商的反馈信息
            return responseText;
        } catch (Exception e) {
            e.printStackTrace();
            // 出现异常则放回null
            return null;
        } finally {
            // 关闭流信息
            try {
                response.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
