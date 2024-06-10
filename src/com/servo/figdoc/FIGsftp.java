/*

FILE_NAME   : FIGsftp.java
AUTHOR NAME : Vinith

    
   SL-NO       CHANGE_ID          DATE                     DISCRIPTION  
   ------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
    1           CH-D-2003       01-03-2024                  * Renaming Docs in SFTP 


 */
package com.servo.figdoc;

import au.com.bytecode.opencsv.CSVWriter;
import com.itextpdf.text.Document;
import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfStream;
import com.itextpdf.text.pdf.PdfWriter;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.servo.businessdoc.SessionService;
import com.servo.configdoc.EJbContext;
import com.servo.configdoc.LoadConfig;
import com.servo.config.SRVTimerServiceConfig;
import com.servo.configdoc.propertyConfig;
import com.servo.dms.entity.NodeDocument;
import com.servo.dms.entity.NodeFolder;
import com.servo.dms.module.remote.DocumentRemoteModule;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Execute;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.TimerService;
import javax.imageio.ImageIO;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import oracle.jdbc.OracleTypes;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.Selectors;
import org.apache.commons.vfs.impl.StandardFileSystemManager;
import org.apache.commons.vfs.provider.sftp.SftpFileSystemConfigBuilder;

@TimerService
public class FIGsftp {

    propertyConfig configObj = new propertyConfig();
    SessionService service = new SessionService();
    HashMap tokenMap = new HashMap();
    String sessionId = "";
    @Resource
    private SRVTimerServiceConfig Timer_Service_Id;
//    SubmitInstance obj1 = new SubmitInstance();
    String errorFlag = "";

    @PostConstruct
    public void loadConfigurations(Connection con1) throws NoSuchProviderException, MessagingException {
        try {
            System.out.println("[init] - Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(configObj);

            System.out.println("SftpIp - " + configObj.getDmsSftpIp());
            System.out.println("SftpUserId - " + configObj.getDmsSftpUserId());
            System.out.println("SftpDir - " + configObj.getDmsSftpDir());

            tokenMap = service.getSession(configObj.getIstreamsIp(), configObj.getIstreamsPort(), "DMS_USER", "system123#");
            if (tokenMap.get("status").equals("1")) {
                sessionId = tokenMap.get("sessionId").toString();
            }
            System.out.println("[init] - Post construct Executed Config Loaded For Doc Upload.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[init] - Config Load Exception->" + ex);
        }
    }

//    public static void main(String[] args) {
//        Connection con = null;
//        String query = "";
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
//        long folderId;
//        try {
//            Class.forName("oracle.jdbc.driver.OracleDriver");
//            con = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.104:1521:orcl", "servostreams104", "system123#");
//
//            query = "select FOLDERID from srv_ru_execution where processid = 1376 and PROCESSINSTANCEID = ?";
//            pstmt = con.prepareCall(query);
//            pstmt.setString(1, "MF-00000000814-PRO");
//            rs = pstmt.executeQuery();
//            rs.next();
//            folderId = rs.getLong("FOLDERID");
//            System.out.println(folderId);
//
//            pstmt.close();
//            rs.close();
////            EJBContext ee = new EJBContext();
//
////            DocumentRemoteModule documentModule = (DocumentRemoteModule) ee.lookup("DocumentModuleServiceImpl", "SRV-DMS-APP", "192.168.1.6", "8080");
////            DocumentRemoteModule documentModule = (DocumentRemoteModule) EJbContext.getContext("DocumentModuleServiceImpl", "SRV-DMS-APP", "192.168.1.6", "8080");
////
////            NodeFolder nf = new NodeFolder();
////            nf.setUuid(folderId);
////
////            List<NodeDocument> nodeDoc = documentModule.getDocumentList("4545", nf);
//        } catch (Exception ex) {
//            System.out.println("system--->" + ex);
//        } finally {
//            try {
//                con.close();
//            } catch (SQLException ex) {
//                Logger.getLogger(initDocUpload.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
//    }
    @Execute
    public void execute(Connection con) throws Exception {

        int sftpstatus = 0;
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long folderId = 0;
        String pinstid = null;
        Map dataMap = new HashMap();
        CallableStatement callableStatement = null;
        String getDMSUploadSql = "{call FIGUPLOADTOSFTP(?,?)}";
        String fig_cif = "";
        try {
            /*
            System.out.println("[DOC Upload] -- Going To Fetch Case For Doc Archival.");

            //CH-D-2000 
            //query = "SELECT A.PROCESSINSTANCEID,A.FOLDERID,A.activityid,B.CUSTOMER_ID,B.LOAN_APPLICATION_NO,B.ACCOUNTNO,B.CIFID,B.TEMPDOC_FLAG,B.UPLOADDOC_FLAG,B.TEMPDOC_LOCATION FROM srv_ru_execution A , MF_EXT B where A.PROCESSINSTANCEID = B.PINSTID and A.processid = ? and A.activityname = ? and (B.TEMPDOC_FLAG IS NULL OR B.UPLOADDOC_FLAG IS NULL) and rownum <= ? order by PROCESSINSTANCEID asc";
//            query = "SELECT A.PROCESSINSTANCEID,A.FOLDERID,A.activityid,B.CUSTOMER_ID,B.LOAN_APPLICATION_NO,NVL(B.ACCOUNTNO,(select BANK_AC_NO from mf_exist_cif_details where BRNET_CLIENT_ID=B.CUSTOMER_ID)) AS ACCOUNTNO,NVL(b.cifid,(select CIF from mf_exist_cif_details where BRNET_CLIENT_ID=B.CUSTOMER_ID)) AS CIFID,B.TEMPDOC_FLAG,B.UPLOADDOC_FLAG,B.TEMPDOC_LOCATION FROM srv_ru_execution A , MF_EXT B where A.PROCESSINSTANCEID = B.PINSTID and A.processid = ? and A.activityname = ? and (B.TEMPDOC_FLAG IS NULL OR B.UPLOADDOC_FLAG IS NULL) and rownum <= ? order by PROCESSINSTANCEID asc";
            query = "SELECT FOLDERID,PROCESSINSTANCEID,ACCOUNTNO,CIFID,TEMPDOC_FLAG,UPLOADDOC_FLAG,TEMPDOC_LOCATION FROM MF_RU_DMSARCHIVAL"
                    + " WHERE EXECUTION_FLAG = ? and rownum <= ? order by PROCESSINSTANCEID asc";
            pstmt = con.prepareCall(query);
            pstmt.setString(1, "I");
//            pstmt.setString(2, Timer_Service_Id.getId());
            pstmt.setString(2, "1");
            rs = pstmt.executeQuery(); */
            callableStatement = con.prepareCall(getDMSUploadSql);
            callableStatement.setString(1, "1");
            callableStatement.registerOutParameter(2, OracleTypes.CURSOR);
            callableStatement.executeUpdate();
            rs = (ResultSet) callableStatement.getObject(2);

            while (rs.next()) {

                List<String> missingDocuments = new ArrayList<>();

                //List<String> foundDocument = new ArrayList<>();
                List<String> size_zero = new ArrayList<>();

                folderId = rs.getLong("FOLDERID");
                pinstid = rs.getString("PROCESSINSTANCEID");
                System.out.println("DOC_EXE DOC Upload] - PROCESSINSTANCEID=" + pinstid);

                dataMap.put("PROCESSINSTANCEID", pinstid);
                dataMap.put("FOLDERID", folderId);
//                dataMap.put("ACTIVITYID", rs.getString("activityid"));
                dataMap.put("ACTIVITYID", "NA");

                //CH-D-2000
                // dataMap.put("ACCOUNTNO", rs.getString("LOAN_APPLICATION_NO"));
                // dataMap.put("CIFID", rs.getString("CUSTOMER_ID"));
                dataMap.put("ACCOUNTNO", rs.getString("ACCOUNTNO"));
                dataMap.put("CIFID", rs.getString("CIFID"));
                dataMap.put("CUSTOMERID", rs.getString("CIFID"));
                dataMap.put("EKYCFLAG", rs.getString("EKYCFLAG"));

                //end
                dataMap.put("TEMPFLAG", (rs.getString("TEMPDOC_FLAG") == null) ? "" : rs.getString("TEMPDOC_FLAG"));
                dataMap.put("UPLOADFLAG", (rs.getString("UPLOADDOC_FLAG") == null) ? "" : rs.getString("UPLOADDOC_FLAG"));
                dataMap.put("TEMPDOC_LOCATION", (rs.getString("TEMPDOC_LOCATION") == null) ? "" : rs.getString("TEMPDOC_LOCATION"));
                System.out.println("[DOC Upload] - TEMPFLAG--FOR-" + pinstid + "--" + dataMap.get("TEMPFLAG").toString());
                System.out.println("[DOC Upload] - UPLOADFLAG--FOR-" + pinstid + "--" + dataMap.get("UPLOADFLAG").toString());
                System.out.println("[DOC Upload] - TEMPDOC_LOCATION--FOR-" + pinstid + "--" + dataMap.get("TEMPDOC_LOCATION").toString());
                if (pinstid != null && folderId != 0) {

                    String fig_pinstid = null;

                    fig_cif = String.valueOf(dataMap.get("CIFID"));

                    String sftpdocs = null;

                    System.out.println("[CKYC  DOC Upload] - Data Fetched Going to initiate Doc Upload");
                    if (dataMap.get("TEMPFLAG").toString().equals("")) {
                        String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                        String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP";
                        String folderToDelete = "EXT-" + deletecustomerID;

                        File directoryToDelete = new File(deletedirectoryPath, folderToDelete);

                        if (directoryToDelete.exists()) {

                            System.out.println("Directory Found -- Deleting Existing Folder before New file creation: " + "EXT-" + deletecustomerID);
                            // Delete the directory
                            deleteDirectory(directoryToDelete);
                        } else {
                            // Directory doesn't exist
                            System.out.println("Directory does not exist.");
                        }

                        int Status = initiateDocUpload(dataMap, folderId, con, pinstid);

                        if (Status == 1) {
                            try {

                                Set<String> foundDocument = new HashSet<>();

                                //File directory = new File(directoryPath);
                                String customerID = dataMap.get("CUSTOMERID").toString();
                                String directoryPath = "DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + customerID;
// Define the names of the documents to check
                                String[] documentNames = {"Photograph.jpg", "Signature.jpg", "UID.pdf"};

                                if (foundDocument != null) {
                                    foundDocument.forEach((fdocs) -> {
                                        System.out.println("[FIG-SFTP]--Documents inside the folder before reading: " + fdocs);
                                    });

                                    foundDocument.clear();
                                } else {
                                    System.out.println("[FIG-SFTP]--Checked the set, safe to add new doc names");
                                }

// Track the names of missing documents
// Check if the directory exists
                                File directory = new File(directoryPath);
                                if (directory.exists() && directory.isDirectory()) {
                                    // List the files in the directory
                                    File[] files = directory.listFiles();
                                    if (files != null) {
                                        // Iterate over the document names
                                        for (String documentName : documentNames) {
                                            boolean found = false;
                                            // Iterate over the files
                                            for (File foundfile : files) {
                                                String fileName = foundfile.getName();
                                                foundDocument.add(fileName);
                                                if (fileName.equalsIgnoreCase(documentName)) {
                                                    found = true;                                                     // Add found document name to the list
                                                    break;
                                                }
                                            }
                                            // If the document is not found, add its name to the list of missing documents
                                            if (!found) {
                                                missingDocuments.add(documentName);
                                            }

                                        }
                                    }
                                }

// Set the status based on the number of missing documents
                                Status = (missingDocuments.size() > 0) ? 1 : 2;

                                String missingDocumentsString = String.join(", ", missingDocuments);

                                //System.out.println("The missing documents are: " + missingDocumentsString );
                                sftpdocs = String.join(", ", foundDocument);

// Print the names of missing documents
                                if (!missingDocuments.isEmpty()) {
                                    System.out.println("The following documents are missing:");
                                    for (String missingDocument : missingDocuments) {
                                        System.out.println("[FIG ARCH] The Document missing is: " + missingDocument);
                                    }
                                    try {
                                        fig_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));

                                        fig_cif = String.valueOf(dataMap.get("CIFID"));

//                                        String dquery = "DELETE FROM FIG_SFTP_FAIL WHERE PINSTID =?";
//                                        PreparedStatement dpstmt = con.prepareStatement(dquery);
//                                        dpstmt.setString(1, fig_pinstid);
//                                        dpstmt.executeUpdate();
//                                        dpstmt.close();
//
//                                        System.out.println("Deleted Entry from Fig_Ckyc_Sftp for: " + fig_pinstid);
                                        String iquery = "Insert Into FIG_SFTP_FAIL (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Missing_Docs,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(iquery)) {
                                            ipstmt.setString(1, fig_pinstid);
                                            ipstmt.setString(2, fig_cif);
                                            ipstmt.setString(3, "NO");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, missingDocumentsString);
                                            ipstmt.setString(6, "FAIL");
                                            ipstmt.execute();
                                            System.out.println("[FIG ARCH]--ERROR INSERTED INTO TABLE FIG_SFTP_FAIL FOR" + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        }

                                        String uquery = "UPDATE FIG_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "MISS");
                                            upstmt.setString(2, "FILES NOT FOUND");
                                            upstmt.setString(3, fig_pinstid);
                                            upstmt.executeUpdate();
                                            System.out.println("[FIG ARCH]--ERROR UPDATED THE TABLE FIG_DOCEXE_SFTP FOR" + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        }

                                        String extquery = "UPDATE FIG_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, missingDocumentsString + " Not FOUND");
                                            extstmt.setString(3, fig_pinstid);
                                            extstmt.executeUpdate();
                                            System.out.println("[FIG ARCH]--ERROR UPDATED THE TABLE FIG_EXT FOR" + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        }

//                                        con.commit();
                                        System.out.println("Inserted in to Fig_Ckyc_Sftp");

                                        sftpstatus = 3;
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                } else {
                                    System.out.println("All required documents exist in the folder.");
                                    if (directory.exists() && directory.isDirectory()) {
                                        File[] files = directory.listFiles();
                                        if (files != null) {
                                            for (File file : files) {
                                                if (file.isFile() && file.length() == 0) {
                                                    System.out.println("Zero kb file found: " + file.getName());
                                                    size_zero.add(file.getName());
                                                }
                                            }
                                        } else {
                                            System.out.println("Directory is empty.");
                                        }
                                    } else {
                                        System.out.println("Directory does not exist or is not a directory.");
                                    }
                                    sftpstatus = (size_zero.size() > 0) ? 2 : 1;
                                    //sftpstatus = 1;
                                    System.err.println("sftpstatus: " + sftpstatus);
                                }

// Use the 'status' variable as needed (send it to another method, etc.)
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            System.err.println("sftpstatus for SFTP initiation: " + sftpstatus);

                            fig_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));

                            fig_cif = String.valueOf(dataMap.get("CIFID"));

                            try {
                                switch (sftpstatus) {
                                    case 1: {
                                        fig_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
                                        System.out.println("[DOC_EXE Doc Upload] -- Proceding For Sftp.");
                                        initiateSftp(configObj, con, dataMap);
                                        //                                    String dquery = "DELETE FROM FIG_SFTP_SUCCESS WHERE PINSTID = ?";
//                                    PreparedStatement dpstmt = con.prepareStatement(dquery);
//                                    dpstmt.setString(1, fig_pinstid);
//                                    dpstmt.executeUpdate();
//                                    dpstmt.execute();
//                                    dpstmt.close();
                                        String squery = "Insert Into FIG_SFTP_SUCCESS (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(squery)) {
                                            ipstmt.setString(1, fig_pinstid);
                                            ipstmt.setString(2, fig_cif);
                                            ipstmt.setString(3, "YES");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, "PASS");
                                            ipstmt.execute();

                                            System.out.println("[FIG ARCH]INSERTED INTO TABLE FIG_SFTP_SUCCESS FOR" + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        }
                                        String extquery = "UPDATE FIG_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "PASS");
                                            extstmt.setString(2, "FILES MOVED TO SFTP");
                                            extstmt.setString(3, fig_pinstid);
                                            extstmt.executeUpdate();
                                            System.out.println("[FIG ARCH] UPDATED THE TABLE TABLE FIG_EXT FOR" + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        }
                                        String uquery = "UPDATE FIG_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "UD");
                                            upstmt.setString(2, "FILES MOVED TO SFTP");
                                            upstmt.setString(3, fig_pinstid);
                                            upstmt.executeUpdate();
                                            System.out.println("[FIG ARCH]UPDATED THE TABLE FIG_DOCEXE_SFTP FOR" + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        }

                                        System.out.println("SFTP MOVEMENT IS SUCCESS FOR CIFID: " + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        break;
                                    }
                                    case 2: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);
                                        String iquery = "Insert Into FIG_SFTP_FAIL (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Missing_Docs,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(iquery)) {
                                            ipstmt.setString(1, fig_pinstid);
                                            ipstmt.setString(2, fig_cif);
                                            ipstmt.setString(3, "NO");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, size_zero.toString());
                                            ipstmt.setString(6, "FAIL");
                                            ipstmt.execute();
                                        }
                                        String uquery = "UPDATE FIG_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "0kb File found");
                                            upstmt.setString(3, fig_pinstid);
                                            upstmt.executeUpdate();
                                        }
                                        String extquery = "UPDATE FIG_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, "0 Kb file detected");
                                            extstmt.setString(3, fig_pinstid);
                                            extstmt.executeUpdate();
                                        }
                                        System.out.println("SFTP MOVEMENT IS FAILED FOR CIFID: " + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        break;
                                    }
                                    default: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);
                                        String uquery = "UPDATE FIG_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "File Upload issue");
                                            upstmt.setString(3, fig_pinstid);
                                            upstmt.executeUpdate();
                                        }
                                        System.out.println("SFTP MOVEMENT IS FAILED FOR CIFID: " + fig_cif + " HAVING PINSTID: " + fig_pinstid);
                                        break;
                                    }
                                }
//                                foundDocument.clear();
                                sftpdocs = null;

                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        } else if (errorFlag.equalsIgnoreCase("DONE")) {
                            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "SKIP", "");
                        } else {
                            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", "ERROR WHILE FETHCING DOCS FROM DMS");
                        }
                    }

//                    else if (dataMap.get("TEMPFLAG").toString().equals("DONE") && dataMap.get("UPLOADFLAG").toString().equals("")) {
//                        initiateSftp(configObj, con, dataMap);
//                    }
                }
//                missingDocuments.clear();
//                foundDocument.clear();
//                size_zero.clear();
            }

            closeCallableConnection(rs, callableStatement);
        } catch (SQLException ex) {
            System.out.println("[FIG-SFTP]  Inside Exception - " + ex);
        } finally {
            closeCallableConnection(rs, callableStatement);
        }
    }

    public int initiateDocUpload(Map dataMap, Long folderId, Connection con, String pinstid) {

        //CH-D-2002
        Map Documents = new HashMap();
        //End
        ByteArrayInputStream is;
        int status = 0;
        try {
            DocumentRemoteModule documentModule = (DocumentRemoteModule) EJbContext.getContext("DocumentModuleServiceImpl", "SRV-DMS-APP", configObj.getDmsIp(), configObj.getDmsPort());
            System.out.println("[CKYC  DOC Upload] - DMS Ejb Object Created.");
            System.out.println("DMS-IP=" + configObj.getDmsIp());
            System.out.println("DMS-Port=" + configObj.getDmsPort());
            NodeFolder nf = new NodeFolder();
            nf.setUuid(folderId);
            System.out.println("DOC_EXE DOC Upload] - sessionId-" + sessionId);

            tokenMap = checkDMSSession(con, status, configObj);
            if (tokenMap.get("status").equals("1")) {
                sessionId = tokenMap.get("sessionId").toString();
            }

            if (!sessionId.equals("")) {
                List<NodeDocument> nodeDoc = documentModule.getDocumentList(sessionId, nf);
                System.out.println("[CKYC DOC Upolad] - Document Details Fetched.");
                if (!nodeDoc.isEmpty()) {

                    for (NodeDocument nodeD : nodeDoc) {
                        System.out.println(nodeD.getUuid());
                        System.out.println(nodeD.getName());
                        dataMap.put("DocName", nodeD.getName());
                        dataMap.put("EXT", nodeD.getExt());
                        Long versn = Long.valueOf(nodeD.getCurrentVersion().getVersion());

                        if (nodeD.getName().equalsIgnoreCase("Group Photo.") || nodeD.getName().equalsIgnoreCase("Group Photo")) {
                            System.out.println("File Skipped");
                        } else {
                            is = new ByteArrayInputStream(documentModule.getDocumentContent(sessionId, nodeD.getUuid(), versn));
                            System.out.println("DOC_EXE DOC Upload] - Going to extract Docs.");
                            //CH-D-2001
                            String docExtName = dataMap.get("DocName").toString();
                            System.out.println("DOC_EXE DOC Upload] - docExtName---" + docExtName);

                            String DocTypeName = docExtName;
                            dataMap.put("ProofName", docExtName);
                            dataMap.put("CsvName", DocTypeName);

                            //Group Photo.
                            if (docExtName.equalsIgnoreCase("Group Photo.")) {
                                docExtName = "Group Photo";

                                dataMap.put("ProofName", docExtName);
                                dataMap.put("CsvName", docExtName);
                                // dataMap.put("CsvName", "Group Photo");
                            }
                            if (docExtName.equalsIgnoreCase("Customer Photo")) {
                                docExtName = "Photograph";
                                dataMap.put("ProofName", docExtName);

                                dataMap.put("CsvName", "Customer Photo");
                            }
                            status = fileOperation(is, dataMap, con);

                            //END
                            is.close();

                            if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "Copy", dataMap.get("TEMPLOC").toString())) {
                                status = 1;

                            }

                        }  // else close
                    }
                    System.out.println("DOC_EXE DOC Upload] -- All Doc Extracted Initiating C-Kyc");

//                    try {
//                        //CH-D-2001
//                        //cKyc(dataMap);
//                        //cKyc(dataMap, con, pinstid);
//                        cKycupdate(dataMap, con, pinstid);
//                        //end
//                    } catch (Exception ex) {
//                        ex.printStackTrace();
//                    }
                } else if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "")) {
//                    configObj.setActivityId(Integer.parseInt(dataMap.get("ACTIVITYID").toString()));
//                    RoutingResponse routeResponse = obj1.submitPInstance(con, dataMap.get("PROCESSINSTANCEID").toString(), configObj);
//                    if (routeResponse.getStatus() == 1) {
//                        System.out.println("[DOC UPLOAD] -- Routing Done for - " + dataMap.get("PROCESSINSTANCEID").toString());
//                    }
                }

            } else {
                System.out.println("DOC_EXE DOC Upload] - Session Not Found To Upload Docs");
            }
        } catch (Exception ex) {
            System.out.println("DOC_EXE DOC Upload] - EX -" + ex);
            if (ex.getMessage().contains("com.servo.dms.exception.RepositoryException: File Not Found for")) {
                errorFlag = "DONE";
            }
            return status;
        }
        return status;
    }

    public int fileOperation(ByteArrayInputStream is, Map dataMap, Connection con) {

        String cykc_filename = null;
        CSVWriter writer = null;
        int status = 0;
        try {
            String docName = "";
            String extension = "";
            File file = new File("DMS_ARCHIVAL");
            if (!file.exists()) {
                boolean b = file.mkdirs();
                System.out.println("DOC_EXE DOC Upload] - Base Directory Created.");
            }

            System.out.println("---Absolute path-1-" + file.getAbsolutePath());

            String customerID = dataMap.get("CUSTOMERID").toString();
            String directoryPath = "DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + customerID;
            File directory = new File(directoryPath);

//            if (directory.exists()) {
//                deleteDirectory(directory);
//                System.out.println("DELETED EXISTING CKYC FILE FOR FIG:");
//                try {
//                    file = new File("DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
//                    System.out.println("New FIG CYKC FILE CREATED:");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            } else {
//                try {
//                    file = new File("DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
//                    System.out.println("NO EXISTING FILES FOUND/NEW FIG CYKC FILE CREATED:");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//            if (directory.mkdir()) {
//                System.out.println("Directory created successfully: " + directoryPath);
//            } else {
//                System.err.println("Failed to create directory: " + directoryPath);
//                // Add error handling as needed
//            }
            file = new File("DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            //System.out.println("----"+);
            configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            dataMap.put("TEMPLOC", configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            System.out.println("---Absolute path-2---" + file.getAbsolutePath());
            if (!file.exists()) {
                file.mkdir();
                System.out.println("DOC_EXE DOC Upload] - Child Folder Created.");
                writer = new CSVWriter(new FileWriter(csv), '|', CSVWriter.NO_QUOTE_CHARACTER);
                String[] header = "FILENAME,DC.WINAME,DC.ACCNO,DC.CIFNO,DOCTYPE".split(",");
                writer.writeNext(header);
                writer.close();
            }
            docName = dataMap.get("DocName").toString();
            extension = dataMap.get("EXT").toString();
            System.out.println("DOC_EXE DOC Upload]-- Extension found- " + extension);
            System.out.println("DOC_EXE DOC Upload]-- DocumentName- " + docName);
            //CH-D-2001
            String docExtName = dataMap.get("ProofName").toString();
            String DocTypeName = dataMap.get("CsvName").toString();
            System.out.println("DOC_EXE DOC Upload]-- File opertaion docExtName- " + docExtName);
            System.out.println("DOC_EXE DOC Upload]-- File opertaion docExtName- " + DocTypeName);

            if (docExtName.equalsIgnoreCase("FIG CUSTOMER PHOTO")) {
                cykc_filename = "Photograph";
            } else if (docExtName.equalsIgnoreCase("FIG PAN CARD")) {
                cykc_filename = "Pancard";
            } else if (docExtName.equalsIgnoreCase("FIG SIGNATURE OR THUMB IMPRESSION")) {
                cykc_filename = "Signature";
            } else {
                cykc_filename = docExtName;
            }

            if (extension.equalsIgnoreCase("pdf")) {
                byte[] array = new byte[is.available()];
                is.read(array);
                //CH-D-2001
                // FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension);
                FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension);
                System.out.println("CKYC UPLOAD FILE - DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension);
                //END
                fos.write(array);
                fos.close();
            } else {
                BufferedImage bImageFromConvert = ImageIO.read(is);
                //CH-D-2001
                //ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension));
                ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension));
                System.out.println(("CKYC UPLOAD IMAGE - DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension));
//END
            }

            //END
//            if (extension.equalsIgnoreCase("pdf")) {
//                byte[] array = new byte[is.available()];
//                is.read(array);
//                //CH-D-2001
//                // FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension);
//                FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension);
//                System.out.println("CKYC UPLOAD FILE - DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension);
//                //END
//                fos.write(array);
//                fos.close();
//            } else {
//                BufferedImage bImageFromConvert = ImageIO.read(is);
//                //CH-D-2001
//                //ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension));
//                ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension));
//                System.out.println(("CKYC UPLOAD IMAGE - DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension));
////END
//            }
            System.out.println("[DOC Upload] - Doc Copied To Server Locations--" + DocTypeName);

            if (!cykc_filename.trim().startsWith("Address Proof Corress") && !cykc_filename.trim().startsWith("Relation ID Proof") && !cykc_filename.trim().startsWith("Second ID Proof") && !cykc_filename.trim().startsWith("ID Proof") && !cykc_filename.trim().startsWith("Address Proof")) {
                System.out.println("[DOC Upload] - Inside the if loop" + DocTypeName);
                writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
                //CH-D-2001

                //String[] values = (docName + "." + extension + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + docName).split(",");
                String[] values = (cykc_filename + "." + extension + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + cykc_filename).split(",");
                //END
                writer.writeNext(values);
                writer.close();
            }

            System.out.println("[DOC Upload] - Csv Entry Created");

            status = 1;

        } catch (Exception ex) {
            System.out.println("DOC_EXE DOC Upload] - File writing exe - " + ex);
            ex.printStackTrace();
            return status;
        }
        return status;
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // Recursive call for subdirectories
                    deleteDirectory(file);
                } else {
                    // Delete file
                    file.delete();
                }
            }
        }
        // Delete the empty directory
        directory.delete();
    }

    public void cKycupdate(Map dataMap, Connection con, String pinstid) {
        CSVWriter writer = null;
        int status = 0;
        String fileA = "";
        File testFile = null;
        String fileB = "";
        String csv_filename = null;
//        String path = System.getenv("DOMAIN_HOME") + File.separator + "DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        String path = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        try {
            //MY CODE

            Map HashCustProof = getDocuemntsDetails(con, pinstid);
            System.out.println("[DOC Upload] - HashCustProof---" + HashCustProof.toString());
            String IdProoftype = HashCustProof.get("IDPROOFTYPE").toString();
            System.out.println("[DOC Upload] - IdProoftype---" + IdProoftype);
            String AddressProofType = HashCustProof.get("ADDRESS_PROOF_TYPE").toString();
            System.out.println("[DOC Upload] - IdProoftype---" + AddressProofType);

            System.out.println("[DOC Upload] -- Completed the CKYC Documents Chages");

            //end
            File file = new File("DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[Doc Upload] - CKyc-- File-" + file.getAbsolutePath());
            configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[Doc Upload] - TEMPLOC-- SFTP-" + configObj.getDmsSftpLocalDir());
            dataMap.put("TEMPLOC", configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
            System.out.println("[Doc Upload] - CKyc-- csv file-" + csv);
            List<String> files = new ArrayList<String>();
//            String StrVal = "";
//
//            if (dataMap.get("EKYCFLAG").toString().equalsIgnoreCase("Y")) {
//
//                StrVal = "Address proof Corress,Relation ID Proof,Second ID Proof";
//
//            } else {
//
//                StrVal = "Address Proof,ID Proof,Address proof Corress,Relation ID Proof,Second ID Proof";
//
//            }

//            String[] arrSplit = StrVal.split(",");
            String[] StrVal;

            if (dataMap.get("EKYCFLAG").toString().equalsIgnoreCase("Y")) {
                StrVal = new String[]{"Address proof Corress", "Relation ID Proof", "Second ID Proof"};
            } else {
                StrVal = new String[]{"Address Proof", "ID Proof", "Address proof Corress", "Relation ID Proof", "Second ID Proof"};
            }

            String StatusValue = "S";
            String csvName = "";
            String proofName = "";

            for (int i = 0; i < StrVal.length; i++) {
                String strTemp = StrVal[i];

                System.out.println("CKYC Wtring csv file for: " + strTemp);

                System.out.println("1--Working==" + strTemp);
                System.out.println("Status---" + StatusValue);
                if ((IdProoftype.equalsIgnoreCase("1")) && (AddressProofType.equalsIgnoreCase("1")) && StatusValue.equalsIgnoreCase("S") && !dataMap.get("EKYCFLAG").toString().equalsIgnoreCase("Y")) {

                    fileA = path + File.separator + "Address Proof Front.jpg";
                    fileB = path + File.separator + "Address Proof Back.jpg";
                    csvName = "Address_ID Proof";

                    proofName = "UID.pdf";
                    StatusValue = "Y";
                    System.out.println("2--Working==");
                } else if (strTemp.equalsIgnoreCase("Address Proof")) {
                    System.out.println("3--Working==");
                    fileA = path + File.separator + "Address Proof Front.jpg";
                    fileB = path + File.separator + "Address Proof Back.jpg";
                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    if ((AddressProofType.equalsIgnoreCase("1")) && !StatusValue.equalsIgnoreCase("Y")) {

                        proofName = "Aadhar.pdf";

                    } else if ((AddressProofType.equalsIgnoreCase("2"))) {
                        proofName = "Voters_Identity_Card.pdf";

                    } else if ((AddressProofType.equalsIgnoreCase("4"))) {
                        proofName = "Passport.pdf";

                    } else if ((AddressProofType.equalsIgnoreCase("5"))) {
                        proofName = "Utility_Bill.pdf";

                    } else if ((AddressProofType.equalsIgnoreCase("6"))) {
                        proofName = "Driving_License.pdf";

                    } else if ((AddressProofType.equalsIgnoreCase("7"))) {
                        proofName = "NREGA_Job_Card.pdf";

                    } else if ((AddressProofType.equalsIgnoreCase("8"))) {
                        proofName = "Utility_Bill.pdf";

                    } else if ((AddressProofType.equalsIgnoreCase("3"))) {
                        proofName = "Other.pdf";

                    }

                    System.out.println("FIG CKYC ID ADDRESS PROOF : " + fileA);
                    System.out.println("FIG CKYC ID ADDRESS PROOF : " + fileB);

                    StatusValue = "N";
                } else if (strTemp.equalsIgnoreCase("ID Proof")) {
                    System.out.println("4--Working==");
                    fileA = path + File.separator + "ID Proof Front.jpg";
                    fileB = path + File.separator + "ID Proof Back.jpg";
                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    if ((IdProoftype.equalsIgnoreCase("1") && !StatusValue.equalsIgnoreCase("Y"))) {

                        proofName = "UID.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("2"))) {
                        proofName = "Voters_Identity_Card.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("3"))) {
                        proofName = "Pancard.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("4"))) {
                        proofName = "Driving_License.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("5"))) {
                        proofName = "Pension_Order.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("6"))) {
                        proofName = "Passport.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("8"))) {
                        proofName = "NREGA_Job_Card.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("9"))) {
                        proofName = "Bank_Statement.pdf";

                    } else if ((IdProoftype.equalsIgnoreCase("3"))) {
                        proofName = "Other.pdf";

                    }

                    System.out.println("FIG CKYC ID PROOF FILE : " + fileA);

                    System.out.println("FIG CKYC ID PROOF FILE : " + fileB);

                    StatusValue = "N";
                } else if (strTemp.equalsIgnoreCase("Address proof Corress") || strTemp.equalsIgnoreCase("Relation ID Proof") || strTemp.equalsIgnoreCase("Second ID Proof")) {
                    System.out.println("5--Working==");

                    fileA = path + File.separator + strTemp + " Front.jpg";
                    fileB = path + File.separator + strTemp + " Back.jpg";

                    System.out.println("FIG CKYC ADDRESS PROOF CORRESS: " + fileA);
                    System.out.println("FIG CKYC ADDRESS PROOF CORRESS: " + fileB);
                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    proofName = strTemp + ".pdf";
                    StatusValue = "Y";
                }

                System.out.println("FIG CKYC DOCUMENTS NAME: " + proofName);

                testFile = new File(fileA);

                if (testFile.exists()) {
                    System.out.println("FIG CKYC FILE EXISTS ");
                    files.add(fileA);
                    files.add(fileB);
                    //ImageToPDF(files, path + File.separator + "Address Proof.pdf");
                    System.out.println("-fileA--" + fileA);
                    System.out.println("-fileA--" + fileB);
                    System.out.println("Pdf uploda csvname" + csvName);

                    System.out.println("CKYC UPLOADED FILES: " + path + File.separator + proofName);

                    String csv_fileid = null;
                    if (proofName.equalsIgnoreCase("FIG CUSTOMER PHOTO.jpg")) {
                        csv_filename = "Photograph.jpg";
                        csv_fileid = "Photograph";
                    } else if (proofName.equalsIgnoreCase("FIG PAN CARD.jpg")) {
                        csv_filename = "Pancard.jpg";
                        csv_fileid = "Pancard";
                    } else if (proofName.equalsIgnoreCase("FIG SIGNATURE OR THUMB IMPRESSION.jpg")) {
                        proofName = "Signature.jpg";
                        csv_fileid = "Signature";
                    } else {
                        csv_filename = proofName;
                        csv_fileid = csvName;
                    }

                    ImageToPDF(files, path + File.separator + csv_filename);

                    String[] values = (csv_filename + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + csv_fileid).split(",");
                    writer.writeNext(values);
                    if (StatusValue.equalsIgnoreCase("Y")) {
                        DeleteFile(files);
                        files.clear();
                        fileA = path + File.separator + "ID Proof Front.jpg";
                        fileB = path + File.separator + "ID Proof Back.jpg";
                        files.add(fileA);
                        files.add(fileB);
                        DeleteFile(files);
                    } else {
                        DeleteFile(files);
                    }

                    files.clear();
                } else {
                    System.out.println("[Doc Upload] - CKyc-- Doc Not Found-Address Proof");
                }

            }
            writer.close();

        } catch (FileNotFoundException fx) {
            System.out.println("Updating File not found");
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", "File is not created");
            System.out.println("Updating File ");
        } catch (Exception ex) {
            System.out.println("[Doc Upload] - CKyc Ex- " + ex);

            ex.printStackTrace();

        }

    }

    public static void ImageToPDF(List<String> inputFiles, String outputFile) {

        try {
            Document document = null;
            FileOutputStream fos = new FileOutputStream(outputFile);

            Image img = Image.getInstance(inputFiles.get(0));
            Rectangle rc = new Rectangle(0, 0, img.getWidth(), img.getHeight());
            img = null;
            document = new Document(rc);
            PdfWriter writer = PdfWriter.getInstance(document, fos);
            writer.setCompressionLevel(PdfStream.BEST_COMPRESSION);
            document.open();
            for (int i = 0; i < inputFiles.size(); i++) {
                img = Image.getInstance(inputFiles.get(i));
                rc = new Rectangle(0, 0, img.getWidth(), img.getHeight());
                document.setPageSize(rc);
                document.newPage();
                img.setAbsolutePosition(0, 0);
                document.add(img);
                img = null;
            }
            document.close();
            writer.close();
            fos.close();
        } catch (Exception e) {
            System.out.println("DOC_EXE DOC Upload] - C Kyc Doc Write Exc- " + e);
        }
    }

    public void initiateSftp(propertyConfig configObj, Connection con, Map dataMap) {
        StandardFileSystemManager manager = new StandardFileSystemManager();

        String serverAddress = configObj.getDmsSftpIp();
        String userId = configObj.getDmsSftpUserId();
        String password = configObj.getDmsSftpPswd();

        //PROD DIRECTORY
        String remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/IN24/";
        //UAT DIRECTORY
        //String remoteDirectory = configObj.getDmsSftpDir();
        //String remoteDirectory = "SERVOSYS/ESAFUAT/BPMDOCUPLOAD/IN24/";
        // String remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/IN24/";
        //NEW DIRECTORY
        //String remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/FIG/IN/";
        System.out.println("DOC_EXE SFTP IP: " + serverAddress);
        System.out.println("DOC_EXE userId: " + userId);
        System.out.println("DOC_EXE password: " + password);
        System.out.println("[CKYC DOC Upload Remote Dir]--" + remoteDirectory);
        String localDirectory = "DMS_ARCHIVAL/FIG/DOCEXE_SFTP/";
        String fileToFTP = configObj.getDmsSftpLocalDir();

        Session session = null;
        Channel channel = null;

        try {

            JSch jsch = new JSch();
            session = jsch.getSession(userId, serverAddress, 22);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");

            session.connect();

            channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp cSftp = (ChannelSftp) channel;

            if (fileToFTP == null) {
                fileToFTP = dataMap.get("TEMPDOC_LOCATION").toString();
            }

            System.out.println("DOC_EXE DOC Upload] - Local Dir -" + localDirectory + fileToFTP);

            System.out.println("DOC_EXE DOC Upload] - check if the file exists");
            String filepath = localDirectory + fileToFTP;
            File file = new File(filepath);
            if (!file.exists()) {
                throw new RuntimeException("CKYC Error. Local file not found");
            }

            System.out.println("DOC_EXE DOC Upload] - Initializes the file manager");
            manager.init();

            System.out.println("DOC_EXE DOC Upload] - Setup our SFTP configuration");
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(
                    opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

            password = URLEncoder.encode(password);

            System.out.println("DOC_EXE DOC Upload] - Create the SFTP URI using the host name, userid, password,  remote path and file name");
            String sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                    + remoteDirectory + fileToFTP;

            System.out.println("DOC_EXE DOC Upload] --" + sftpUri);

            System.out.println("DOC_EXE DOC Upload] - Create local file object");
            FileObject localFile = manager.resolveFile(file.getAbsolutePath());

            System.out.println("DOC_EXE DOC Upload] - Create remote file object");
            FileObject remoteFile = manager.resolveFile(sftpUri, opts);

            System.out.println("DOC_EXE DOC Upload] -- Copy local file to sftp server");
            remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
            System.out.println("DOC_EXE DOC Upload] -- Folder upload successful");

            File localDir = new File(localDirectory + fileToFTP);
            File[] subFiles = localDir.listFiles();
            for (File item : subFiles) {
                sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                        + remoteDirectory + fileToFTP + File.separator + item.getName();

                file = new File(localDirectory + fileToFTP + File.separator + item.getName());
                System.out.println("DOC_EXE DOC Upload] -- " + file.getAbsolutePath());
                localFile = manager.resolveFile(file.getAbsolutePath());
                remoteFile = manager.resolveFile(sftpUri, opts);
                if (item.isFile()) {
                    remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                    cSftp.chmod(511, remoteDirectory + fileToFTP + "/" + item.getName());
                    System.out.println("DOC_EXE DOC Upload] -- File upload successful");
                }
            }

            cSftp.chmod(511, remoteDirectory + "/" + dataMap.get("TEMPDOC_LOCATION").toString());
            System.out.println("DOC_EXE DOC Upload] -- Updating Flag.");

//            try {
//                String deletecustomerID = dataMap.get("CUSTOMERID").toString();
//                String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP";
//                String folderToDelete = "EXT-" + deletecustomerID;
//
//                File directoryToDelete = new File(deletedirectoryPath, folderToDelete);
//
//                if (directoryToDelete.exists()) {
//
//                    System.out.println("Directory Found -- Deleting Folder After SFTP " + "EXT-" + deletecustomerID);
//                    // Delete the directory
//                    deleteDirectory(directoryToDelete);
//                } else {
//                    // Directory doesn't exist
//                    System.out.println("Directory does not exist to -- Deleting Folder After SFTP.");
//                }
//            } catch (Exception e) {
//                e.getCause();
//            }
            try {
                if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "")) {
                    String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                    String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "FIG" + File.separator + "DOCEXE_SFTP";
                    String folderToDelete = "EXT-" + deletecustomerID;

                    File directoryToDelete = new File(deletedirectoryPath, folderToDelete);

                    if (directoryToDelete.exists()) {

                        System.out.println("Directory Found -- Deleting Existing Folder before New file creation: " + "EXT-" + deletecustomerID);
                        // Delete the directory
                        deleteDirectory(directoryToDelete);
                    } else {
                        // Directory doesn't exist
                        System.out.println("Directory does not exist.");
                    }
                }
            } catch (Exception e) {
                e.getCause();
            }
        } catch (Exception ex) {
            System.out.println("DOC_EXE DOC Upload] --File upload Ex - " + ex);
        } finally {
            // CH-D-2001
            // manager.close();
            //end

            manager.close();
            channel.disconnect();
            session.disconnect();
        }

    }

    public static void removeDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                for (File aFile : files) {
                    removeDirectory(aFile);
                }
            }
            dir.delete();
        } else {
            dir.delete();
        }
    }

    public boolean updateFlag(Connection con, String pinstId, String type, String tempLocation) {
        String query = "";
        PreparedStatement pstmt = null;
        try {
            if (type.equalsIgnoreCase("Copy")) {
                query = "UPDATE FIG_EXT SET TEMPDOC_FLAG = ?,TEMPDOC_LOCATION = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "DONE");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);
            } else if (type.equalsIgnoreCase("Upload")) {
                query = "UPDATE FIG_EXT SET UPLOADDOC_FLAG = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "DONE");
                pstmt.setString(2, pinstId);

            } else if (type.equalsIgnoreCase("BYPASS")) {
                query = "UPDATE FIG_EXT SET UPLOADDOC_FLAG = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "BYPASS");
                pstmt.setString(2, pinstId);
            } else if (type.equalsIgnoreCase("UPD")) {
                query = "UPDATE FIG_DOCEXE_SFTP SET EXECUTION_FLAG = ? ,SIG_PIC_UPTIME=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                //Changed By Anurag 02-01-2019 for changing the IN and response OUT23
                //pstmt.setString(1, "U");
                pstmt.setString(1, "UD");
                //End by Anurag
                pstmt.setString(2, pinstId);
            } else if (type.equalsIgnoreCase("CKYC_MISSING")) {
                query = "UPDATE FIG_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? ,SIG_PIC_UPTIME=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "MISS");
                pstmt.setString(2, "FILES NOT FOUND");
                pstmt.setString(3, pinstId);
            } else if (type.equalsIgnoreCase("EXC")) {
                query = "UPDATE FIG_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? ,SIG_PIC_UPTIME=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "EXC");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);
            }
            pstmt.executeUpdate();
            closeConnection(null, pstmt);
        } catch (Exception ex) {
            System.out.println("DOC_EXE DOC Upload] -- UPdate Status Eex - " + ex);
            return Boolean.FALSE;
        } finally {
            closeConnection(null, pstmt);
        }
        return Boolean.TRUE;
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

    //CH-D-2001  
    public Map getDocuemntsDetails(Connection con, String strPinstid) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String branchName = "";
        Map dataProof = new HashMap();
        try {
            query = "SELECT IDPROOFTYPE,ADDRESS_PROOF_TYPE FROM FIG_EXT WHERE PINSTID=?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, strPinstid);
            rs = pstmt.executeQuery();

            while (rs.next()) {

                dataProof.put("IDPROOFTYPE", rs.getString("IDPROOFTYPE"));
                dataProof.put("ADDRESS_PROOF_TYPE", rs.getString("ADDRESS_PROOF_TYPE"));

            }
            closeConnection(rs, pstmt);
        } catch (Exception ex) {
            return dataProof;
        } finally {
            closeConnection(rs, pstmt);
        }
        return dataProof;
    }

    //End
    public HashMap checkDMSSession(Connection con, int userId, propertyConfig config) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String query = "";
        int count = 0;
        SessionService service = new SessionService();
        HashMap sessionMap = new HashMap();
        try {
            query = "SELECT COUNT(SESSIONID) AS SESSIONCOUNT FROM SRV_RU_CONNECTIONINFO WHERE USERID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setInt(1, userId);

            rs = pstmt.executeQuery();
            rs.next();

            count = rs.getInt("SESSIONCOUNT");
            closeConnection(rs, pstmt);
            if (count == 0) {
                sessionMap = service.getSession(config.getIstreamsIp(), config.getIstreamsPort(), config.getUserName(), "system123#");
            }
        } catch (Exception ex) {

        } finally {
            closeConnection(rs, pstmt);
        }
        return sessionMap;
    }

    private void closeCallableConnection(ResultSet rs, CallableStatement callableStatement) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (callableStatement != null) {
                callableStatement.close();
                callableStatement = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //CH-D-2002

    public void DeleteFile(List<String> files) {
        for (int i = 0; i < files.size(); i++) {
            File ob = new File(files.get(i));
            ob.delete();
        }
    }

}
