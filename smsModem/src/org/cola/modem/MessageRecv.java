
package org.cola.modem;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.cola.util.db.DBUtil;
import org.smslib.InboundBinaryMessage;
import org.smslib.InboundMessage;
import org.smslib.InboundMessage.MessageClasses;
import org.smslib.Service;

/**
 * Description: <br/>
 * Copyright (C), 2001-2014, Jason Chan <br/>
 * This program is protected by copyright laws. <br/>
 * Program Name:MessageRecv <br/>
 * Date:2014年5月25日
 * 
 * @author ChenMan
 * @version 1.0
 */
public class MessageRecv extends Thread {
    private DBUtil dbUtil = new DBUtil();
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private synchronized void doIt() throws Exception {
        List<InboundMessage> msgList;

        try {
            // Service.getInstance().startService();
            msgList = new ArrayList<InboundMessage>();
            Service.getInstance().readMessages(msgList, MessageClasses.ALL);
            for (InboundMessage msg : msgList) {
                if (msg instanceof InboundBinaryMessage) {
                    //InboundBinaryMessage无法使用getText
                    System.out.println(msg);
                } else {
                    try {
                        System.out.println("===============================================================================");
                        System.out.println("<< 接收短信 >>");
                        System.out.println("-------------------------------------------------------------------------------");
                        System.out.println("Date       : " + msg.getDate().toString());
                        System.out.println("Originator : " + msg.getOriginator());
                        System.out.println("Text       : " + msg.getText());
                        System.out.println("===============================================================================");
                        insertSms(msg.getOriginator(), msg.getText(), msg.getDate());
                    } catch (Exception e) {
                        System.out.println(msg);
                    }
                }
                Service.getInstance().deleteMessage(msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Service.getInstance().stopService();
        }
    }

    private void insertSms(String msisdn, String message, Date reqDate)
            throws SQLException {
        StringBuffer sb = new StringBuffer();
        sb.setLength(0);
        sb.append("insert into SmsCat_smsrecv(SysNo,cellnumber,smscontent, HandleTime,fromcom) values(SMSCAT_SYSNO.Nextval,'"
                + msisdn
                + "','"
                + message.replace('\'', '‘')
                + "',to_date('"
                + df.format(reqDate) + "','yyyy-mm-dd hh24:mi:ss'),1)");
        dbUtil.executeUpdate(sb.toString(), 60000);
    }

    public void run() {
        while (true) {
            try {
                doIt();
                sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
