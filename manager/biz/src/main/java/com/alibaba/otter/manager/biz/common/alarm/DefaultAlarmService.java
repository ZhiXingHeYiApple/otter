package com.alibaba.otter.manager.biz.common.alarm;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang.StringUtils;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import com.alibaba.otter.manager.biz.config.parameter.SystemParameterService;
import com.alibaba.otter.shared.common.model.config.parameter.SystemParameter;

/**
 * 发送邮件进行报警
 * 
 * @author jianghang 2013-9-6 上午11:42:04
 * @since 4.2.2
 */
public class DefaultAlarmService extends AbstractAlarmService {

    private static final String    TITLE = "alarm_from_otter";
    private String                 username;
    private JavaMailSender         mailSender;
    // 通过第三方的云片来发送预警短信
    private YunpianMessage yunpianMessage;
    private SystemParameterService systemParameterService;

    @Override
    public void doSend(AlarmMessage data) throws Exception {
        SimpleMailMessage mail = new SimpleMailMessage(); // 只发送纯文本
        mail.setFrom(username);
        mail.setSubject(TITLE);// 主题
        mail.setText(data.getMessage());// 邮件内容
        String receiveKeys[] = StringUtils.split(StringUtils.replace(data.getReceiveKey(), ";", ","), ",");

        SystemParameter systemParameter = systemParameterService.find();
        List<String> mailAddress = new ArrayList<String>();
        List<String> phoneAddress = new ArrayList<String>();
        for (String receiveKey : receiveKeys) {
            String receiver = convertToReceiver(systemParameter, receiveKey);
            String strs[] = StringUtils.split(StringUtils.replace(receiver, ";", ","), ",");
            for (String str : strs) {
                if (isMail(str)) {
                    if (str != null) {
                        mailAddress.add(str);
                    }
                } else if (isSms(str)) {
                    // do nothing
                    phoneAddress.add(str);
                }
            }
        }

        if (!mailAddress.isEmpty()) {
            mail.setTo(mailAddress.toArray(new String[mailAddress.size()]));
            doSendMail(mail);
        }

        if (!phoneAddress.isEmpty()) {
            // 短信不应太长，进行截断
            String simpleMsg = data.getMessage().substring(0, 30);
            for (String phoneNumber : phoneAddress) {
                this.yunpianMessage.sendOtterAlarmMessage(simpleMsg, phoneNumber);
            }
        }
    }

    private void doSendMail(SimpleMailMessage mail) {
        if (mailSender instanceof JavaMailSenderImpl) {
            JavaMailSenderImpl mailSenderImpl = (JavaMailSenderImpl) mailSender;
            if (StringUtils.isNotEmpty(mailSenderImpl.getUsername())
                && StringUtils.isNotEmpty(mailSenderImpl.getPassword())) {
                // 正确设置了账户/密码，才尝试发送邮件
                mailSender.send(mail);
            }
        }
    }

    /**
     * 判断是否是合法的邮件地址
     *
     * @param receiveKey 邮件地址字符串
     * @return true OR false
     */
    private boolean isMail(String receiveKey) {
        return StringUtils.contains(receiveKey, '@');
    }

    /**
     * 判断是否是合法的短信地址
     *
     * @param receiveKey 短信地址
     * @return true OR false
     */
    private boolean isSms(String receiveKey) {
        if (isChinaPhoneLegal(receiveKey)) {
            return true;
        }
        return false;
    }

    /**
     * 手机号码:（关键在于匹配前三位）
     * 13[0-9], 14[5,7], 15[0, 1, 2, 3, 5, 6, 7, 8, 9], 17[0, 1, 6, 7, 8], 18[0-9]
     * 移动号段: 134,135,136,137,138,139,147,150,151,152,157,158,159,170,178,182,183,184,187,188
     * 联通号段: 130,131,132,145,155,156,170,171,175,176,185,186
     * 电信号段: 133,149,153,170,173,177,180,181,189
     *
     * @param str 待验证的手机号字符串
     * @return 手机号是否合法
     * @throws PatternSyntaxException
     */
    private boolean isChinaPhoneLegal(String str) throws PatternSyntaxException {
        String regExp = "^1(3[0-9]|4[57]|5[0-35-9]|7[0135678]|8[0-9])\\d{8}$";
        Pattern p = Pattern.compile(regExp);
        Matcher m = p.matcher(str);
        return m.matches();
    }

    private String convertToReceiver(SystemParameter systemParameter, String receiveKey) {
        if (StringUtils.equalsIgnoreCase(systemParameter.getDefaultAlarmReceiveKey(), receiveKey)) {
            return systemParameter.getDefaultAlarmReceiver();
        } else {
            return systemParameter.getAlarmReceiver().get(receiveKey);
        }
    }

    public void setMailSender(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    public void setYunpianMessage(YunpianMessage yunpianMessage){
        this.yunpianMessage = yunpianMessage;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setSystemParameterService(SystemParameterService systemParameterService) {
        this.systemParameterService = systemParameterService;
    }

}
