package org.cola.modem;

import java.text.SimpleDateFormat;

import org.cola.util.db.DBUtil;
import org.smslib.OutboundMessage;
import org.smslib.Service;
import org.smslib.Message.MessageEncodings;

/**
 * Description: <br/>
 * Copyright (C), 2001-2014, Jason Chan <br/>
 * This program is protected by copyright laws. <br/>
 * Program Name:MessageSendThread <br/>
 * Date:2014年5月25日
 * 
 * @author ChenMan
 * @version 1.0
 */
public class MessageSendThread extends Thread {
    private DBUtil dbUtil = new DBUtil();

    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private String id;

    private String msisdn;

    private String content;

    /**
     * @param id
     * @param msisdn
     * @param content
     */
    public MessageSendThread(String id, String msisdn, String content) {
        super();
        this.id = id;
        this.msisdn = msisdn;
        this.content = content;
    }

    public void run() {
        try {
            OutboundMessage msg = new OutboundMessage(msisdn, content);
            msg.setEncoding(MessageEncodings.ENCUCS2);
            Service.getInstance().sendMessage(msg);
            System.out
                    .println("===============================================================================");
            System.out.println("<< 发送短信 >>");
            System.out
                    .println("-------------------------------------------------------------------------------");
            System.out.println("Date      : " + msg.getDate().toString());
            System.out.println("Recipient : " + msg.getRecipient());
            System.out.println("Text      : " + msg.getText());
            System.out.println("Status    : " + msg.getMessageStatus());
            System.out
                    .println("===============================================================================");
            if (msg != null && msg.getMessageStatus().toString().equals("SENT")) {
                dbUtil.executeUpdate(
                        "update SmsCat_SMS set status=2,retrycount=0,HandleTime=to_date('"
                                + df.format(msg.getDate())
                                + "','yyyy-mm-dd hh24:mi:ss'),tocom=1 where SysNo="
                                + id, 60000);
                MessageSend.decrease();
                System.out.println(">>> 更新记录状态成功!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
