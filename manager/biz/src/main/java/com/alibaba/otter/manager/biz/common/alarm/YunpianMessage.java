package com.alibaba.otter.manager.biz.common.alarm;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

// 云片短信
public class YunpianMessage extends SendMessage {

    /**发送充值成功短信*/
    protected final static String alarmMessage = "【上上签】otter线上数据同步任务出现异常，请紧急排查！%s";

    @Override
    public boolean sendSMSNotify(String content, String phone) {

        // 填写参数
        Map<String, String> paramsMap = new HashMap<String, String>(8);

        paramsMap.put("apikey", "7ebce891586d80f0a6edb95d1d580a96");
        paramsMap.put("text", content);
        paramsMap.put("mobile", phone);

        // 发送短信
        String responseText = super.sendMessage("http://yunpian.com/v1/sms/send.json", paramsMap);

        // 处理发送信息
        return this.dealMessageStatus(responseText);
    }

    /**
     * 发送otter的监控预警信息
     * @param content 监控预警信息
     * @param phone 手机号
     * @return 短信发送是否成功（true OR false）
     */
    public boolean sendOtterAlarmMessage(String content ,String phone) {
        content = String.format(alarmMessage, content);
        return sendSMSNotify(content, phone);
    }

    // 处理返回的信息
    @Override
    public boolean dealMessageStatus(String responseText) {
        JSONObject result = JSONObject.parseObject(responseText);
        // 结果返回 0 则说明发送成功，否则发送失败。
        return (Integer) result.get("code") == 0;
    }

}
