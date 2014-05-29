package org.cola.util.db;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import oracle.sql.BLOB;

/**
 * Description:
 * <br/>Copyright (C), 2001-2014, Jason Chan
 * <br/>This program is protected by copyright laws.
 * <br/>Program Name:DBUtil
 * <br/>Date:2014年3月17日
 * @author	ChenMan
 * @version	1.0
 */
public class DBUtil {
    private ConnectionPool connectionPool = null;

    public DBUtil() {
        this.connectionPool = ConnectionPool.getConnectionPoolInstance();
    }

    private void afterQueryProcess(Statement statement, Connection connection, ResultSet result) {
        try {
            if (result != null) {
                result.close();
                result = null;
            }
        } catch (SQLException ex) {

        }

        try {
            if (statement != null) {
                statement.close();
                statement = null;
            }
        } catch (SQLException ex) {

        }

        try {
            if (connection != null) {
                if (!(connection.isClosed()))
                    connection.close();
                connection = null;
            }
        } catch (SQLException ex) {

        }
    }

    public Map<String, String> getColumns(String sql, int timeOut) {
        HashMap<String, String> hashMap = new HashMap<String, String>();
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            result = statement.executeQuery(sql);
            result.setFetchSize(100);
            ResultSetMetaData rsmd = result.getMetaData();
            int i = rsmd.getColumnCount();
            for (int j = 0; j < i; ++j) {
                String colName = rsmd.getColumnName(j + 1);
                String colType = rsmd.getColumnTypeName(j + 1);
                hashMap.put(colName, colType);
            }
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return hashMap;
    }

    public List<String> getColumnList(String sql, int timeOut) {
        ArrayList<String> list = new ArrayList<String>();
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            result = statement.executeQuery(sql);
            statement.setFetchSize(100);
            ResultSetMetaData rsmd = result.getMetaData();
            int i = rsmd.getColumnCount();
            for (int j = 0; j < i; ++j) {
                String str = rsmd.getColumnName(j + 1);
                list.add(str);
            }
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return list;
    }

    public DBResult executeUpdate(String sql, int timeOut) throws SQLException {
        DBResult dbResult = new DBResult();
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            statement.executeUpdate(sql);
            dbResult.iErrorCode = 0;
            dbResult.iColsCnt = 0;
            dbResult.iRowsCnt = 0;
            dbResult.iTotalCnt = 0;
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
            dbResult.iErrorCode = e.getErrorCode();
            dbResult.sErrorDesc = e.getMessage();
            throw new SQLException("sql:" + sql + " error msg:"
                    + e.getMessage());
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return dbResult;
    }

    public List executeQuery(String sql, ArrayList<ArrayList<String>> list)
            throws SQLException {
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            statement.setQueryTimeout(300);
            result = statement.executeQuery(sql);
            statement.setFetchSize(2000);
            ArrayList<String> alist = null;
            int i = result.getMetaData().getColumnCount();
            while (result.next()) {
                alist = new ArrayList<String>();
                for (int j = 1; j <= i; ++j) {
                    String str = result.getString(j);
                    if (str == null)
                        str = "";
                    alist.add(str.trim());
                }
                list.add(alist);
            }
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
            throw new SQLException("sql:" + sql + " error msg:"
                    + e.getMessage());
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return list;
    }

    public DBResult executeQuery(String sql, int timeOut) throws SQLException {
        DBResult dbResult = new DBResult();
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            result = statement.executeQuery(sql);
            statement.setFetchSize(2000);
            
            ResultSetMetaData rsmd = result.getMetaData();
            dbResult.iColsCnt = rsmd.getColumnCount();
            ArrayList<String> titlelist = new ArrayList<String>();
            for (int j = 0; j < dbResult.iColsCnt; ++j) {
                String str = rsmd.getColumnName(j + 1);
                titlelist.add(str);
            }
            dbResult.titleList = titlelist;
            
            result.last();
            dbResult.iRowsCnt = result.getRow();
            result.beforeFirst();
            dbResult.iTotalCnt = dbResult.iRowsCnt;
            dbResult.aaRes = new String[dbResult.iRowsCnt][dbResult.iColsCnt];
            for (int j = 0; result.next(); ++j)
                for (int k = 1; k <= dbResult.iColsCnt; ++k) {
                    String str = result.getString(k);
                    if (str == null)
                        str = "";
                    dbResult.aaRes[j][k - 1] = str.trim();
                }
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
            dbResult.iErrorCode = e.getErrorCode();
            dbResult.sErrorDesc = e.getMessage();
            throw new SQLException("sql:" + sql + " error msg:"
                    + dbResult.sErrorDesc);
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return dbResult;
    }

    public DBResult executeSql(String sql, int timeOut) throws SQLException {
        String str = "";
        try {
            str = sql.trim().substring(0, 6);
        } catch (Exception localException) {
            str = "";
        }
        if (str.equalsIgnoreCase("select"))
            return executeQuery(sql, timeOut);
        return executeUpdate(sql, timeOut);
    }

    public ByteArrayOutputStream outputBlob(String sql, String colName,
            int timeOut) throws SQLException, IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            statement = connection.createStatement();
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            result = statement.executeQuery(sql);
            if (result.next()) {
                BLOB blob = (BLOB) result.getBlob(colName);
                InputStream inputStream = blob.getBinaryStream();
                byte[] arrayOfByte = new byte[blob.getBufferSize()];
                int i = -1;
                while ((i = inputStream.read(arrayOfByte)) != -1)
                    outStream.write(arrayOfByte, 0, i);
                outStream.flush();
                outStream.close();
            }
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
            throw new SQLException(sql + " msg : " + e.getMessage());
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return outStream;
    }

    public DBResult inputBlob(String sql, String colName,
            InputStream inputStream, int timeOut) throws SQLException,
            IOException {
        DBResult dbResult = new DBResult();
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            result = statement.executeQuery(sql);
            if (result.next()) {
                BLOB blob = (BLOB) result.getBlob(colName);
                OutputStream outputStream = blob.getBinaryOutputStream();
                byte[] arrayOfByte = new byte[blob.getBufferSize()];
                int i = -1;
                while ((i = inputStream.read(arrayOfByte)) != -1)
                    outputStream.write(arrayOfByte, 0, i);
                outputStream.flush();
                outputStream.close();
                connection.commit();
            }
            dbResult.iErrorCode = 0;
            dbResult.iColsCnt = 0;
            dbResult.iRowsCnt = 0;
            dbResult.iTotalCnt = 0;
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
            dbResult.iErrorCode = -100;
            dbResult.sErrorDesc = e.getMessage();
            throw new SQLException(sql + " msg : " + e.getMessage());
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return dbResult;
    }

    public DBResult executeBatch(List<String> sqls, int timeOut)
            throws SQLException {
        DBResult dbResult = new DBResult();
        dbResult.iErrorCode = -100;
        dbResult.sErrorDesc = "初始化";
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            String str = "";
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            Iterator<String> it = sqls.iterator();
            while (it.hasNext())
                try {
                    str = (String) it.next();
                    if (timeOut > 0)
                        statement.setQueryTimeout(timeOut);
                    statement.addBatch(str);
                } catch (SQLException e2) {
                    connection.rollback();
                    dbResult.iErrorCode = e2.getErrorCode();
                    dbResult.sErrorDesc = "executeUpdate()==>" + str;
                    afterQueryProcess(statement, connection, result);
                    dbResult.iErrorCode = e2.getErrorCode();
                    throw e2;
                }
            statement.executeBatch();
            statement.clearBatch();
            connection.commit();
            dbResult.iErrorCode = 0;
            dbResult.sErrorDesc = "执行成功！";
        } catch (SQLException e1) {
            afterQueryProcess(statement, connection, result);
            throw e1;
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return dbResult;
    }

    public DBResult executeUpdateTrans(String sql, int timeOut)
            throws SQLException {
        DBResult dbResult = new DBResult();
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            statement.executeUpdate(sql);
            dbResult.iErrorCode = 0;
            dbResult.iColsCnt = 0;
            dbResult.iRowsCnt = 0;
            dbResult.iTotalCnt = 0;
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            afterQueryProcess(statement, connection, result);
            dbResult.iErrorCode = e.getErrorCode();
            throw new SQLException(sql + " msg : " + e.getMessage());
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return dbResult;
    }
    
    /**
     * ORACLE分页查询
     * @param sql
     * @param timeOut
     * @param start
     * @param rowCnt
     * @return
     * @throws SQLException
     */
    public DBResult executeQueryByPage(String sql, int timeOut, int start,
            int rowCnt) throws SQLException {
        Statement statement = null;
        ResultSet result = null;
        Connection connection = null;
        DBResult dbResult = new DBResult();

        StringBuffer sb = new StringBuffer(1024);
        sb.append("select * from ( select rownum rownum_, row_.* from (");
        sb.append(sql);
        sb.append(" ) row_").append(" where rownum < ").append(start + rowCnt)
                .append(" ) where rownum_ >= ").append(start);
        try {
            connection = this.connectionPool.getConnection();
            statement = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            if (timeOut > 0)
                statement.setQueryTimeout(timeOut);
            result = statement.executeQuery(sb.toString());
            statement.setFetchSize(2000);
            dbResult.iColsCnt = ((ResultSetMetaData) result.getMetaData())
                    .getColumnCount() - 1;
            result.last();
            dbResult.iRowsCnt = result.getRow();
            result.beforeFirst();
            dbResult.aaRes = new String[dbResult.iRowsCnt][dbResult.iColsCnt];

            for (int k = 0; result.next(); ++k) {
                for (int l = 1; l <= dbResult.iColsCnt; ++l) {
                    String str = result.getString(l + 1);
                    if (str == null)
                        str = "";
                    dbResult.aaRes[k][(l - 1)] = str.trim();
                }
            }
        } catch (SQLException e) {
            afterQueryProcess(statement, connection, result);
            dbResult.iErrorCode = -100;
            throw new SQLException("sql:" + sql + " error msg:"
                    + e.getMessage());
        } finally {
            afterQueryProcess(statement, connection, result);
        }
        return dbResult;
    }

    public List executeProc(String sql,
            ArrayList<Map<String, Object>> paramList, int timeOut)
            throws SQLException {
        DBResult dbResult = new DBResult();
        ResultSet result = null;
        Connection connection = null;
        CallableStatement callableStatement = null;
        List<String> list = new ArrayList<String>();
        try {
            connection = this.connectionPool.getConnection();
            callableStatement = connection.prepareCall(sql);
            Vector<Integer> vector = new Vector<Integer>();
            for (int i = 0; i < paramList.size(); ++i) {
                Map<String, Object> map = (Map) paramList.get(i);
                String paramType = (String) map.get("proc_param_type");
                Object param = map.get("proc_param");
                if (param instanceof Integer) {
                    if ("proc_param_type_out".equals(paramType)) {
                        callableStatement.registerOutParameter(i + 1, 4);
                        vector.add(Integer.valueOf(i + 1));
                    } else {
                        callableStatement.setInt(i + 1,
                                ((Integer) param).intValue());
                    }
                } else if (param instanceof Double) {
                    if ("proc_param_type_out".equals(paramType)) {
                        callableStatement.registerOutParameter(i + 1, 8);
                        vector.add(Integer.valueOf(i + 1));
                    } else {
                        callableStatement.setDouble(i + 1,
                                ((Double) param).doubleValue());
                    }
                } else if (param instanceof Long) {
                    if ("proc_param_type_out".equals(paramType)) {
                        callableStatement.registerOutParameter(i + 1, -5);
                        vector.add(Integer.valueOf(i + 1));
                    } else {
                        callableStatement.setLong(i + 1,
                                ((Long) param).longValue());
                    }
                } else if (param instanceof Float) {
                    if ("proc_param_type_out".equals(paramType)) {
                        callableStatement.registerOutParameter(i + 1, 6);
                        vector.add(Integer.valueOf(i + 1));
                    } else {
                        callableStatement.setFloat(i + 1,
                                ((Float) param).floatValue());
                    }
                } else if ("proc_param_type_out".equals(paramType)) {
                    callableStatement.registerOutParameter(i + 1, 12);
                    vector.add(Integer.valueOf(i + 1));
                } else {
                    callableStatement.setString(i + 1, (String) param);
                }
            }
            if (timeOut > 0)
                callableStatement.setQueryTimeout(timeOut);
            boolean bool = callableStatement.execute();
            if (bool)
                dbResult.iErrorCode = 0;
            else
                dbResult.iErrorCode = -1;
            dbResult.iColsCnt = 0;
            dbResult.iRowsCnt = 0;
            dbResult.iTotalCnt = 0;
            Iterator<Integer> it = vector.iterator();
            while (it.hasNext()) {
                list.add(callableStatement.getString(it.next()));
            }
        } catch (SQLException localSQLException) {
            afterQueryProcess(callableStatement, connection, result);
            localSQLException.printStackTrace();
            dbResult.iErrorCode = localSQLException.getErrorCode();
            throw localSQLException;
        } finally {
            afterQueryProcess(callableStatement, connection, result);
        }
        return list;
    }

    public DBResult executeProc(String sql, List<Object> paramList, int timeOut)
            throws SQLException {
        DBResult dbResult = new DBResult();
        ResultSet result = null;
        Connection connection = null;
        CallableStatement cs = null;
        try {
            connection = this.connectionPool.getConnection();
            cs = connection.prepareCall(sql);
            for (int i = 0; i < paramList.size(); ++i) {
                Object param = paramList.get(i);
                if (param instanceof Integer)
                    cs.setInt(i + 1, ((Integer) param).intValue());
                else if (param instanceof Double)
                    cs.setDouble(i + 1, ((Double) param).doubleValue());
                else if (param instanceof Long)
                    cs.setLong(i + 1, ((Long) param).longValue());
                else if (param instanceof Float)
                    cs.setFloat(i + 1, ((Float) param).floatValue());
                else
                    cs.setString(i + 1, (String) param);
            }
            if (timeOut > 0)
                cs.setQueryTimeout(timeOut);
            boolean bool = cs.execute();
            if (bool)
                dbResult.iErrorCode = 0;
            else
                dbResult.iErrorCode = -1;
            dbResult.iColsCnt = 0;
            dbResult.iRowsCnt = 0;
            dbResult.iTotalCnt = 0;
        } catch (SQLException e) {
            afterQueryProcess(cs, connection, result);
            dbResult.iErrorCode = e.getErrorCode();
            throw e;
        } finally {
            afterQueryProcess(cs, connection, result);
        }
        return dbResult;
    }

    public String getSeq(String sql) throws SQLException {
        StringBuffer sb = new StringBuffer("select ").append(sql).append(
                ".nextval s_srl from dual");
        DBResult dbResult = executeQuery(sb.toString(), 300);
        String str = null;
        if (dbResult.iErrorCode == 0)
            str = dbResult.aaRes[0][0];
        else
            str = "0";
        return str;
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            connection = this.connectionPool.getConnection();
        } catch (Exception localException) {
        }
        return connection;
    }
}