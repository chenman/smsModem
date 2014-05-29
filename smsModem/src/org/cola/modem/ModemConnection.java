package org.cola.modem;

import org.smslib.AGateway;
import org.smslib.ICallNotification;
import org.smslib.IGatewayStatusNotification;
import org.smslib.IInboundMessageNotification;
import org.smslib.IOrphanedMessageNotification;
import org.smslib.IOutboundMessageNotification;
import org.smslib.InboundMessage;
import org.smslib.OutboundMessage;
import org.smslib.Service;
import org.smslib.AGateway.GatewayStatuses;
import org.smslib.AGateway.Protocols;
import org.smslib.Message.MessageTypes;
import org.smslib.modem.SerialModemGateway;

/**
 * Description:
 * <br/>Copyright (C), 2001-2014, Jason Chan
 * <br/>This program is protected by copyright laws.
 * <br/>Program Name:ModemConnection
 * <br/>Date:2014年4月29日
 * @author	ChenMan
 * @version	1.0
 */
public class ModemConnection {
    public void setGateway() throws Exception {
        
        OutboundNotification outboundNotification = new OutboundNotification();
        InboundNotification inboundNotification = new InboundNotification();
        CallNotification callNotification = new CallNotification();
        GatewayStatusNotification statusNotification = new GatewayStatusNotification();
        OrphanedMessageNotification orphanedMessageNotification = new OrphanedMessageNotification();

        try {
            SerialModemGateway gateway = new SerialModemGateway("modem.com1", "COM1", 9600, "SIEMENS", "");
            gateway.setProtocol(Protocols.PDU);
            gateway.setInbound(true);
            gateway.setOutbound(true);
            gateway.setSimPin("0000");
            gateway.setSmscNumber("+8613800591500");
            
            // Set up the notification methods.
            Service.getInstance().setInboundMessageNotification(inboundNotification);
            Service.getInstance().setCallNotification(callNotification);
            Service.getInstance().setGatewayStatusNotification(statusNotification);
            Service.getInstance().setOrphanedMessageNotification(orphanedMessageNotification);
            Service.getInstance().setOutboundMessageNotification(outboundNotification);

            Service.getInstance().addGateway(gateway);
            Service.getInstance().startService();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class OutboundNotification implements IOutboundMessageNotification {
        public void process(AGateway gateway, OutboundMessage msg) {
            System.out.println("Outbound handler called from Gateway: "
                    + gateway.getGatewayId());
            System.out.println(msg);
        }
    }

    public class InboundNotification implements IInboundMessageNotification {
        public void process(AGateway gateway, MessageTypes msgType,
                InboundMessage msg) {
            if (msgType == MessageTypes.INBOUND)
                System.out.println(">>> New Inbound message detected from Gateway: "
                                + gateway.getGatewayId());
            else if (msgType == MessageTypes.STATUSREPORT)
                System.out.println(">>> New Inbound Status Report message detected from Gateway: "
                                + gateway.getGatewayId());
            System.out.println(msg);
        }
    }

    public class CallNotification implements ICallNotification {
        public void process(AGateway gateway, String callerId) {
            System.out.println(">>> New call detected from Gateway: "
                    + gateway.getGatewayId() + " : " + callerId);
        }
    }

    public class GatewayStatusNotification implements
            IGatewayStatusNotification {
        public void process(AGateway gateway, GatewayStatuses oldStatus,
                GatewayStatuses newStatus) {
            System.out.println(">>> Gateway Status change for "
                    + gateway.getGatewayId() + ", OLD: " + oldStatus
                    + " -> NEW: " + newStatus);
        }
    }

    public class OrphanedMessageNotification implements
            IOrphanedMessageNotification {
        public boolean process(AGateway gateway, InboundMessage msg) {
            System.out.println(">>> Orphaned message part detected from "
                    + gateway.getGatewayId());
            System.out.println(msg);
            return true; // Delete Orphaned Message.
        }
    }
}
