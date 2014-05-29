
package org.cola.modem;

import org.cola.util.db.DBResult;
import org.cola.util.db.DBUtil;
/**
 * Description:
 * <br/>Copyright (C), 2001-2014, Jason Chan
 * <br/>This program is protected by copyright laws.
 * <br/>Program Name:MessageSend
 * <br/>Date:2014年5月25日
 * @author	ChenMan
 * @version	1.0
 */
public class MessageSend extends Thread {

    private DBUtil dbUtil = new DBUtil();
    private static int forSendMsgCnt = 0;
    
    public synchronized static void decrease() {
        --forSendMsgCnt;
    }

    private synchronized void doIt() throws Exception {
        try {
            // Service.getInstance().startService();
            String sql = "Select SYSNO, CELLNUMBER, SMSCONTENT from SmsCat_SMS where Status=0 and rownum < 10 order by Priority desc";
            DBResult result = dbUtil.executeQuery(sql, 60000);
            forSendMsgCnt = result.iRowsCnt;
            for (int i = 0; i < result.iRowsCnt; ++i) {
                new MessageSendThread(result.aaRes[i][0], result.aaRes[i][1],
                        result.aaRes[i][2]).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Service.getInstance().stopService();
        }
    }

    public void run() {
        while (true) {
            try {
                if (forSendMsgCnt == 0) {
                    doIt();
                }
                sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
