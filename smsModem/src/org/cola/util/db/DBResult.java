package org.cola.util.db;

import java.util.List;

/**
 * Description:
 * <br/>Copyright (C), 2001-2014, Jason Chan
 * <br/>This program is protected by copyright laws.
 * <br/>Program Name:DBResult
 * <br/>Date:2014年3月17日
 * @author	ChenMan
 * @version	1.0
 */
public class DBResult {
    public int iErrorCode;
    public String sErrorDesc;
    public int iColsCnt;
    public int iRowsCnt;
    public int iTotalCnt;
    public String[][] aaRes;
    public List<String> titleList;
}
