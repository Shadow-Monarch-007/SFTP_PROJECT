/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.servo.businessdoc.SessionService;
import com.servo.configdoc.LoadConfig;
import com.servo.configdoc.propertyConfig;
import com.servo.db_con.DBUtil;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.*;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import oracle.jdbc.OracleTypes;

/**
 *
 * @author VINITH
 */
@TimerService
public class Backlog_doc_found {

    propertyConfig configObj = new propertyConfig();
    SessionService service = new SessionService();
    HashMap tokenMap = new HashMap();
    String sessionId = "";

    @PostConstruct
    public void loadConfigurations(Connection con1) throws NoSuchProviderException, MessagingException {
        try {
            System.out.println("[BL-DOC-CHECK]-- Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(configObj);

            System.out.println("[BL-DOC-CHECK]--SftpIp - " + configObj.getDmsSftpIp());
            System.out.println("[BL-DOC-CHECK]--SftpUserId - " + configObj.getDmsSftpUserId());
            System.out.println("[BL-DOC-CHECK]--SftpDir - " + configObj.getDmsSftpDir());
            System.out.println("[BL-DOC-CHECK]--BaclogIn - " + configObj.getBacklogsftpIn());

            tokenMap = service.getSession(configObj.getIstreamsIp(), configObj.getIstreamsPort(), "DMS_USER", "system123#");
            if (tokenMap.get("status").equals("1")) {
                sessionId = tokenMap.get("sessionId").toString();
            }
            System.out.println("[BL-DOC-CHECK]-- Post construct Executed Config Loaded For Doc Upload.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[BL-DOC-CHECK]-- Config Load Exception->" + ex);
        }
    }

    @Execute
    public void execute(Connection con) throws Exception {
        CallableStatement callableStatement = null;
        String getDMSUploadSql = "{call CKYC_DOC_CHECK(?)}";
        Map HashMap = null;

        try {
            callableStatement = con.prepareCall(getDMSUploadSql);
            callableStatement.registerOutParameter(1, OracleTypes.CURSOR);
            callableStatement.executeUpdate();
            try (ResultSet rs = (ResultSet) callableStatement.getObject(2)) {
                while (rs.next()) {
                    String CIFID = rs.getString("CIFID");
                    PICWORKITEM(CIFID, con);

                }
            }
        } catch (SQLException e) {
            System.out.println("BL-DOC-CHECK]--" + e.getMessage());
//            HashMap.put("REMARKS", e.getMessage());
//            HashMap.put("REMARKS", e.getMessage());
//            updateFlag(con, HashMap);

        }
        closeCallableConnection(callableStatement);

    }

    private void PICWORKITEM(String CIFID, Connection con) {
        String[] tables = {"MF_EXT", "FIG_EXT", "MBL_EXT", "CASA_EXT", "MF_EXTHISTORY", "FIG_EXTHISTORY", "MBL_EXTHISTORY", "CASA_EXTHISTORY"};
        for (String table : tables) {
            if (table.equals("CASA_EXT") || table.equals("CASA_EXTHISTORY")) {
                try (Connection CASAconnection = DBUtil.getCASAConnection()) {
                    CheckforDocuments(CASAconnection, CIFID, table);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                CheckforDocuments(con, CIFID, table);
            }

        }
    }

    public boolean CheckforDocuments(Connection con, String cifId, String tableName) {
        System.out.println("[ALLCKYC-RESP]--CIFID TO BE UPDATED: " + cifId);

        //String query = "SELECT PINSTID FROM " + tableName + " WHERE CIFID = ? ORDER BY ";
        String Pqur = "SELECT PINSTID FROM(SELECT PINSTID FROM " + tableName + " WHERE CIFID = '170000047705' ORDER BY PROMOTIONAL_MEETING_DATE DESC) WHERE ROWNUM<=1";
//        String query = "SELECT COUNT(processinstanceid) AS CASE_COUNT FROM " + tableName + " WHERE STATUS IS NULL AND CIFID = ?";
        try (PreparedStatement pstmt = con.prepareStatement(Pqur)) {
            pstmt.setString(1, cifId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String work_item = rs.getString("PINSTID");
                    if (work_item != null) {
                        String updateSql = "UPDATE " + tableName + " SET Status = ?, Remarks = ? ,CKYC_RESP_TIME = SYSDATE WHERE Cifid = ?";
                        try (PreparedStatement updStmt = con.prepareStatement(updateSql)) {
//                            updStmt.setString(1, status);
//                            updStmt.setString(2, remarks);
                            updStmt.setString(3, cifId);
                            int rowsAffected = updStmt.executeUpdate();
                            if (rowsAffected > 0) {
                                System.out.println("[ALLCKYC-RESP]--RESPONSE IS SUCCESSFULLY UPDATED FOR: " + cifId);
                                return true;
                            } else {
                                System.out.println("[ALLCKYC-RESP]--COULDN'T UPDATE THE RESPONSE FOR: " + cifId);
                                return false;
                            }
                        }
                    } else {
                        System.out.println("[ALLCKYC-RESP]--NO ENTRY FOR THE CIF: " + cifId + " IN THE TABLE:  " + tableName);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("[ALLCKYC-RESP]--EXCEPTIKON OCCURED WHILE TRYING TO UPDATE THE TABLE: " + tableName);
            e.printStackTrace();
        }
        return false;
    }

    private void closeCallableConnection(CallableStatement callableStatement) {
        try {
            if (callableStatement != null) {
                callableStatement.close();
                callableStatement = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void closeConnection(ResultSet rs, PreparedStatement ps) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (ps != null) {
                ps.close();
                ps = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean updateFlag(Connection con, Map HashMap) {
        String query = "";
        PreparedStatement pstmt = null;
        String type = HashMap.get("CallType").toString();
        String REMARKS = HashMap.get("REMARKS").toString();
        String M_DOCS = HashMap.get("M_DOCS").toString();
        String CIFID = HashMap.get("CIFID").toString();
        try {

            query = "UPDATE ALL_CIF_DOC_CHECK SET REMARKS = ?,FOUND_M_DOCS = ? WHERE CIFID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, REMARKS);
            pstmt.setString(2, M_DOCS);
            pstmt.setString(3, CIFID);
            pstmt.executeUpdate();
            closeConnection(null, pstmt);
        } catch (SQLException ex) {
            System.out.println("[DOC Upload] -- UPdate Status Eex - " + ex);
            return Boolean.FALSE;
        } finally {
            closeConnection(null, pstmt);
        }
        return Boolean.TRUE;
    }

}
