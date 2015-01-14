package org.cola.modem;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
    
    private int times;
    
    private final static int SENT = 1;

    private final static int FAILED = -1;
    
    private final static int ERROR = -2;

    /**
     * @param id
     * @param msisdn
     * @param content
     */
    public MessageSendThread(String id, String msisdn, String content, int times) {
        super();
        this.id = id;
        this.msisdn = msisdn;
        this.content = content;
        this.times = times;
    }
    
    private void setSendStatus(int status, Date handleTime) {
    	try {
    		StringBuffer sb = new StringBuffer();
    		sb.append("update SmsCat_SMS set HandleTime=to_date('"
			                + df.format(handleTime)
			                + "','yyyy-mm-dd hh24:mi:ss'),tocom=1");
    		if (SENT == status || times <= 0) {
    			sb.append(",status=" + status);
    		} else {
    			sb.append(",retrycount=retrycount-1");
    		}
    		sb.append(" where SysNo=" + id);
			dbUtil.executeUpdate(sb.toString(), 60000);
		} catch (SQLException e) {
			e.printStackTrace();
		}
        MessageSend.decrease();
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
            	setSendStatus(SENT, msg.getDate());
                System.out.println(">>> 更新记录状态成功!");
            } else if (msg != null && msg.getMessageStatus().toString().equals("FAILED")){
            	setSendStatus(FAILED, msg.getDate());
                System.out.println(">>> 发送失败!");
            } else {
            	setSendStatus(ERROR, msg.getDate());
                System.out.println(">>> 无法发送!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
