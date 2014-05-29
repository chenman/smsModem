package org.cola.modem;

/**
 * Description: <br/>
 * Copyright (C), 2001-2014, Jason Chan <br/>
 * This program is protected by copyright laws. <br/>
 * Program Name:Main <br/>
 * Date:2014年4月29日
 * 
 * @author ChenMan
 * @version 1.0
 */
public class SmsModem {

    public static void main(String args[]) {
        ModemConnection mc = new ModemConnection();
        try {
            mc.setGateway();
            new MessageRecv().start();
            new MessageSend().start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
