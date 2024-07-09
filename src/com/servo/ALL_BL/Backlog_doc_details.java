/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.ALL_BL;

import com.servo.businessdoc.SessionService;
import com.servo.configdoc.LoadConfig;
import com.servo.configdoc.propertyConfig;
import com.servo.db_con.DBUtil;
import java.io.File;
import java.nio.file.*;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Execute;
import javax.annotation.PostConstruct;
import javax.annotation.TimerService;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import oracle.jdbc.OracleTypes;
import static org.apache.commons.io.FilenameUtils.removeExtension;

@TimerService
public class Backlog_doc_details {

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
            System.out.println("[BL-DOC-CHECK]--BaclogIn - " + configObj.getBacklogsftpIn());
            System.out.println("[BL-DOC-CHECK]--62KCIF_DIR - " + configObj.getLocalDirectry());
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
        //Map HashMap = null;

        try {
            callableStatement = con.prepareCall(getDMSUploadSql);
            callableStatement.registerOutParameter(1, OracleTypes.CURSOR);
            callableStatement.executeUpdate();
            try (ResultSet rs = (ResultSet) callableStatement.getObject(1)) {
                while (rs.next()) {
                    String CIFID = rs.getString("CIFID");
                    System.out.println("BL-DOC-CHECK]--GOING TO READ THE FILES FOR THE CIF: " + CIFID);
                    READ_DIR(CIFID, con);

                }
            }
        } catch (SQLException e) {
            System.out.println("[BL-DOC-CHECK]--EX: " + e.getMessage());

        }
        closeCallableConnection(callableStatement);

    }

    private void READ_DIR(String CIFID, Connection con) {

        Set<String> foundDocument = new HashSet<>();
        //String fileNameWithoutExtension;
        Map HaMap = new HashMap();

        Path dir = Paths.get("D:\\62K_Record");
        String folder_to_read = dir.toString() + File.separator + CIFID;
        File directory = new File(folder_to_read);

        if (directory.exists() && directory.isDirectory()) {
            // List the files in the directory
            File[] files = directory.listFiles();
            if (files != null) {
                // Iterate over the files
                for (File foundFile : files) {
                    //fileNameWithoutExtension = removeExtension(foundFile.getName());
                    foundDocument.add(foundFile.getName());
                    //foundDocument.add(fileNameWithoutExtension);
                }
                String available_docs = String.join(", ", foundDocument);
                HaMap.put("DOC_IN", available_docs);
                HaMap.put("CIFID", CIFID);
                boolean upstatus = updateFlag(con, HaMap);
                if (upstatus) {
                    System.out.println("[BL-DOC-CHECK]--DOCUMENTS AVAILABLE ARE UPDATED FOR: " + CIFID);
                } else {
                    System.out.println("[BL-DOC-CHECK]--DOCUMENTS AVAILABLE COULDN'T BE AVAILABLE FOR: " + CIFID);
                }

            } else {
                System.err.println("[BL-DOC-CHECK]--Error: Unable to list files in directory.");
            }
        } else {
            System.err.println("[BL-DOC-CHECK]--Error: Directory does not exist or is not a directory.");
        }
    }

    public boolean updateFlag(Connection con, Map HashMap) {
        String query = "";
        PreparedStatement pstmt = null;
        //String type = HashMap.get("CallType").toString();
        //String REMARKS = HashMap.get("REMARKS").toString();
        //String M_DOCS = HashMap.get("M_DOCS").toString();
        String DOCS_IN_SYSTEM = HashMap.get("DOC_IN").toString();
        String CIFID = HashMap.get("CIFID").toString();
        try {

            query = "UPDATE ALL_CIF_DOC_CHECK SET DOCS_IN_SYSTEM = ? WHERE CIFID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, DOCS_IN_SYSTEM);
            pstmt.setString(2, CIFID);
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

}
