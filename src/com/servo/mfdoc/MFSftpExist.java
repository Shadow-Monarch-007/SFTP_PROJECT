/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.mfdoc;

import com.servo.otherdoc.SubmitInstance;
import au.com.bytecode.opencsv.CSVWriter;
import com.itextpdf.text.Document;
import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfStream;
import com.itextpdf.text.pdf.PdfWriter;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.servo.businessdoc.SessionService;
import com.servo.configdoc.EJbContext;
import com.servo.configdoc.LoadConfig;
import com.servo.config.SRVTimerServiceConfig;
import com.servo.configdoc.propertyConfig;
import com.servo.dms.entity.NodeDocument;
import com.servo.dms.entity.NodeFolder;
import com.servo.dms.exception.InvalidRepositoryException;
import com.servo.dms.exception.PathNotFoundException;
import com.servo.dms.module.remote.DocumentRemoteModule;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Execute;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.TimerService;
import javax.imageio.ImageIO;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import oracle.jdbc.OracleTypes;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.Selectors;
import org.apache.commons.vfs.impl.StandardFileSystemManager;
import org.apache.commons.vfs.provider.sftp.SftpFileSystemConfigBuilder;

/**
 *
 * @author VINITH
 */
@TimerService
public class MFSftpExist {

    propertyConfig configObj = new propertyConfig();
    SessionService service = new SessionService();
    HashMap tokenMap = new HashMap();
    String sessionId = "";
    @Resource
    private SRVTimerServiceConfig Timer_Service_Id;
    SubmitInstance obj1 = new SubmitInstance();
    String errorFlag = "";

    String ExqueryString = "";

    @PostConstruct
    public void loadConfigurations(Connection con1) throws NoSuchProviderException, MessagingException {
        try {
            System.out.println("[MF-SFTP]-- Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(configObj);

            System.out.println("[MF-SFTP]--SftpIp - " + configObj.getDmsSftpIp());
            System.out.println("[MF-SFTP]--SftpUserId - " + configObj.getDmsSftpUserId());
            System.out.println("[MF-SFTP]--SftpDir - " + configObj.getDmsSftpDir());

            tokenMap = service.getSession(configObj.getIstreamsIp(), configObj.getIstreamsPort(), "DMS_USER", "system123#");
            if (tokenMap.get("status").equals("1")) {
                sessionId = tokenMap.get("sessionId").toString();
            }
            System.out.println("[MF-SFTP]-- Post construct Executed Config Loaded For Doc Upload.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[MF-SFTP]-- Config Load Exception->" + ex);
        }
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

    @Execute
    public void execute(Connection con) throws Exception {

        int sftpstatus = 0;
        System.out.println("[MF-SFTP]--MERGE  Start Calling the Docupload");

        String missingDocumentsString = "";
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long folderId = 0;
        String pinstid = null;
        String LoanType = null;
        Map dataMap;
        CallableStatement callableStatement = null;
        String getDMSUploadSql = "{call MFUPLOADTOSFTP(?,?)}";

        System.out.println("[MF-SFTP]--INSIDE EXECUTE");
        try {

            callableStatement = con.prepareCall(getDMSUploadSql);
            callableStatement.setString(1, "1");
            callableStatement.registerOutParameter(2, OracleTypes.CURSOR);
            callableStatement.executeUpdate();
            rs = (ResultSet) callableStatement.getObject(2);

            System.out.println("[MF-SFTP]--MERGE  Dms Upload after getting  --" + configObj.getProcessid());

            while (rs.next()) {
                
                dataMap = new HashMap();

                List<String> missingDocuments = new ArrayList<>();

                //List<String> foundDocument = new ArrayList<>();
                List<String> size_zero = new ArrayList<>();

                folderId = rs.getLong("FOLDERID");
                pinstid = rs.getString("PROCESSINSTANCEID");
                System.out.println("[MF-SFTP]-- - PROCESSINSTANCEID=" + pinstid);
                dataMap.put("PROCESSINSTANCEID", pinstid);
                dataMap.put("FOLDERID", folderId);
                dataMap.put("ACTIVITYID", rs.getString("activityid"));
                dataMap.put("ACCOUNTNO", rs.getString("ACCOUNTNO"));
                dataMap.put("CIFID", rs.getString("CIFID"));
                dataMap.put("CUSTOMERID", rs.getString("CIFID"));
                //dataMap.put("LOAN_TYPE", rs.getString("LOAN_TYPE"));

                dataMap.put("LOAN_TYPE", (rs.getString("LOAN_TYPE") == null) ? "" : rs.getString("LOAN_TYPE"));
                LoanType = rs.getString("LOAN_TYPE");
                //end
                dataMap.put("TEMPFLAG", (rs.getString("TEMPDOC_FLAG") == null) ? "" : rs.getString("TEMPDOC_FLAG"));
                dataMap.put("UPLOADFLAG", (rs.getString("UPLOADDOC_FLAG") == null) ? "" : rs.getString("UPLOADDOC_FLAG"));
                dataMap.put("TEMPDOC_LOCATION", (rs.getString("TEMPDOC_LOCATION") == null) ? "EXT-" + rs.getString("CIFID") : rs.getString("TEMPDOC_LOCATION"));
                System.out.println("[MF-SFTP]--- TEMPFLAG--FOR-" + pinstid + "--" + dataMap.get("TEMPFLAG").toString());
                System.out.println("[MF-SFTP]--- UPLOADFLAG--FOR-" + pinstid + "--" + dataMap.get("UPLOADFLAG").toString());
                System.out.println("[MF-SFTP]--- TEMPDOC_LOCATION--FOR-" + pinstid + "--" + dataMap.get("TEMPDOC_LOCATION").toString());
                System.out.println("[MF-SFTP]--- LOAN_TYPE--FOR-" + pinstid + "--" + LoanType);

                //if (LoanType != null) {
                //if (!LoanType.contains("CCL")) {
                System.out.println("[MF-SFTP]--- LoanType DOESN'T CONTAIN: CCL. PROCEEDING FOR DOCUPLOAD");

//                        if (LoanType.equalsIgnoreCase("WPKL") || LoanType.equalsIgnoreCase("FPKL") || LoanType.equalsIgnoreCase("MPKL")) {
//
//                            System.out.println("[MF-SFTP]--- LoanType is[WPKL/FPKL/MPKL]--SKIPPING DOCUPLOAD");
//
////                        if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "BYPASS", "")) {
//                            if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "SKIP", "")) {
////                                configObj.setActivityId(Integer.parseInt(dataMap.get("ACTIVITYID").toString()));
////                                RoutingResponse routeResponse = obj1.submitPInstance(con, dataMap.get("PROCESSINSTANCEID").toString(), configObj);
////                                if (routeResponse.getStatus() == 1) {
////                                    System.out.println("[MF-SFTP]--[MERGE DOC UPLOAD] error case -- Routing Done for - " + dataMap.get("PROCESSINSTANCEID").toString());
////                                }
//                            }
////
////                        }
//                        } 
                //else {
                System.out.println("[MF-SFTP]--PROCEEDING DOCUPLOAD");

                if (pinstid != null && folderId != 0) {

                    System.out.println("[MF-SFTP]---PINSTID & FOLDER IS IS NOT NULL--PROCEEDING FOR DOCUPLOAD");

                    String mf_pinstid = null;

                    String mf_cif = null;

                    String sftpdocs = null;

                    System.out.println("[MF-SFTP]-- Data Fetched Going to initiate Doc Upload");
                    if (dataMap.get("TEMPFLAG").toString().equals("")) {
                        String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                        String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP";
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
                        System.out.println("[MF-SFTP]-- Status:" + Status);

                        if (Status == 1) {
                            try {

                                Set<String> foundDocument = new HashSet<>();

                                //File directory = new File(directoryPath);
                                String customerID = dataMap.get("CUSTOMERID").toString();
                                String directoryPath = "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + customerID;
// Define the names of the documents to check
                                //String[] documentNames = {"Photograph", "Signature", "UID"};

                                File directory = new File(directoryPath);

                                try {
                                    mf_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
                                    System.out.println("[MFCKYC-UPLOAD]-- SELECTING ID PROOF TYPE FOR PINSTID: " + mf_pinstid);
                                    String idq = "SELECT IDPROOFTYPE AS ID_PROOF_TYPE FROM MF_EXT WHERE PINSTID = ?";
                                    try (PreparedStatement pid = con.prepareStatement(idq)) {
                                        pid.setString(1, mf_pinstid);
                                        try (ResultSet idrs = pid.executeQuery()) {
                                            while (idrs.next()) {
                                                String ID_PROOF_NUMBER = String.valueOf(idrs.getString("ID_PROOF_TYPE"));

                                                System.out.println("[MFCKYC-UPLOAD]--ID_PROOF_NUMBER: " + ID_PROOF_NUMBER);
                                                dataMap.put("ID_PROOF_NUMBER", ID_PROOF_NUMBER);
                                            }
                                        }
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }

                                String idprooftype = dataMap.get("ID_PROOF_NUMBER").toString();

                                System.out.println("[MFCKYC-UPLOAD]--PRIMARYIDPROOF_TYPE: " + idprooftype);

                                String[] documentNames;

                                switch (idprooftype) {
                                    case "1": {
                                        System.out.println("[MFCKYC-UPLOAD]--PRIMARY ID IS AADHAR");
                                        documentNames = new String[]{"Photograph", "Signature", "UID"};
                                        break;
                                    }
                                    case "2": {
                                        System.out.println("[MFCKYC-UPLOAD]--PRIMARY ID IS VOTER");
                                        documentNames = new String[]{"Photograph", "Signature", "Voters_Identity_Card"};
                                        break;
                                    }
                                    case "3": {
                                        System.out.println("[MFCKYC-UPLOAD]--PRIMARY ID IS Pancard");
                                        documentNames = new String[]{"Photograph", "Signature", "Pancard"};
                                        break;
                                    }
                                    case "4": {
                                        System.out.println("[MFCKYC-UPLOAD]--PRIMARY ID IS Driving Licence");
                                        documentNames = new String[]{"Photograph", "Signature", "Driving_License"};
                                        break;
                                    }
                                    case "6": {
                                        System.out.println("[MFCKYC-UPLOAD]--PRIMARY ID IS Passport");
                                        documentNames = new String[]{"Photograph", "Signature", "Passport"};
                                        break;
                                    }
                                    default: {
                                        documentNames = new String[]{"Photograph", "Signature", "UID"};

                                        System.out.println("[MFCKYC-UPLOAD]--ID TYPE IS NEITHER 1 NOR 2NOW WE CONSIDER PRIMARY ID IS AADHAR");
                                        break;
                                    }
                                }

                                if (foundDocument != null) {
                                    foundDocument.forEach((fdocs) -> {
                                        System.out.println("Documents inside the folder before reading: " + fdocs);
                                    });

                                    foundDocument.clear();
                                }

                                if (directory.exists() && directory.isDirectory()) {
                                    // List the files in the directory
                                    File[] files = directory.listFiles();
                                    if (files != null) {
                                        // Iterate over the document names
                                        for (String documentName : documentNames) {
                                            boolean found = false;
                                            // Iterate over the files
                                            for (File foundFile : files) {
                                                String fileNameWithoutExtension = removeExtension(foundFile.getName());
                                                foundDocument.add(foundFile.getName());
                                                if (fileNameWithoutExtension.equalsIgnoreCase(documentName)) {
                                                    found = true;
                                                    // Add found document name to the list
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
                                //Status = (missingDocuments.size() > 0) ? 1 : 2;
                                missingDocumentsString = String.join(", ", missingDocuments);

                                foundDocument.forEach((fdocs) -> {
                                    System.out.println("Documents inside the folder: " + fdocs);
                                });

                                //System.out.println("The missing documents are: " + missingDocumentsString );
                                sftpdocs = String.join(", ", foundDocument);
                                System.out.println("[MF-SFTP]--String of found documents: " + sftpdocs);
// Print the names of missing documents
                                if (!missingDocuments.isEmpty()) {
                                    System.out.println("The following documents are missing:");
                                    missingDocuments.forEach((missingDocument) -> {
                                        System.out.println(missingDocument);
                                    });
                                    try {
                                        mf_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));

                                        System.out.println("Inserted in to MF_Ckyc_Sftp");

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
                                            System.out.println("[MF-SFTP]--Directory is empty.");
                                        }
                                    } else {
                                        System.out.println("[MF-SFTP]--Directory does not exist or is not a directory.");
                                    }
                                    sftpstatus = (size_zero.size() > 0) ? 2 : 1;
                                    //sftpstatus = 1;
                                    System.err.println("sftpstatus: " + sftpstatus);
                                }

// Use the 'status' variable as needed (send it to another method, etc.)
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            System.err.println("[MF-SFTP]--sftpstatus for SFTP initiation: " + sftpstatus);
                            try {
                                mf_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
                                mf_cif = String.valueOf(dataMap.get("CIFID"));

                                switch (sftpstatus) {
                                    case 1: {

                                        System.out.println("[MF-SFTP]--- Proceding For Sftp.");
                                        initiateSftp(configObj, con, dataMap);
                                        String squery = "Insert Into MF_SFTP_SUCCESS (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(squery)) {
                                            ipstmt.setString(1, mf_pinstid);
                                            ipstmt.setString(2, mf_cif);
                                            ipstmt.setString(3, "YES");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, "PASS");
                                            ipstmt.execute();

                                        }
                                        String extquery = "UPDATE MF_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "PASS");
                                            extstmt.setString(2, "FILES MOVED TO SFTP");
                                            extstmt.setString(3, mf_pinstid);
                                            extstmt.executeUpdate();
                                        }

                                        String uquery = "UPDATE MF_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "UD");
                                            upstmt.setString(2, "FILES MOVED TO SFTP");
                                            upstmt.setString(3, mf_pinstid);
                                            upstmt.executeUpdate();
                                        }
                                        break;
                                    }
                                    case 2: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);
                                        String iquery = "Insert Into MF_SFTP_FAIL (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Missing_Docs,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(iquery)) {
                                            ipstmt.setString(1, mf_pinstid);
                                            ipstmt.setString(2, mf_cif);
                                            ipstmt.setString(3, "NO");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, size_zero.toString());
                                            ipstmt.setString(6, "FAIL");
                                            ipstmt.execute();
                                        }

                                        String uquery = "UPDATE MF_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "0kb File found");
                                            upstmt.setString(3, mf_pinstid);
                                            upstmt.executeUpdate();
                                        }

                                        String extquery = "UPDATE MF_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, "0 Kb file detected");
                                            extstmt.setString(3, mf_pinstid);
                                            extstmt.executeUpdate();
                                        }
                                        break;
                                    }
                                    case 3: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);

                                        String iquery = "Insert Into MF_SFTP_FAIL (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Missing_Docs,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(iquery)) {
                                            ipstmt.setString(1, mf_pinstid);
                                            ipstmt.setString(2, mf_cif);
                                            ipstmt.setString(3, "NO");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, missingDocumentsString);
                                            ipstmt.setString(6, "FAIL");
                                            ipstmt.execute();
                                        }

                                        String uquery = "UPDATE MF_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "MISS");
                                            upstmt.setString(2, "FILES NOT FOUND");
                                            upstmt.setString(3, mf_pinstid);
                                            upstmt.executeUpdate();
                                        }

                                        String extquery = "UPDATE MF_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, missingDocumentsString + " Not FOUND");
                                            extstmt.setString(3, mf_pinstid);
                                            extstmt.executeUpdate();
                                        }

                                        break;
                                    }
                                    default: {
                                        System.out.println("[MF DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);
                                        String uquery = "UPDATE MF_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "File Upload issue");
                                            upstmt.setString(3, mf_pinstid);
                                            upstmt.executeUpdate();
                                        }
                                        break;
                                    }
                                }
                                sftpdocs = null;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        } else if (errorFlag.equalsIgnoreCase("DONE")) {
                            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "SKIP", "");
                        } else {
                            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", "ERROR WHILE FETHCING DOCS FROM DMS");
                        }

                    } else if (dataMap.get("TEMPFLAG").toString().equals("DONE") && dataMap.get("UPLOADFLAG").toString().equals("")) {
                        initiateSftp(configObj, con, dataMap);
                    }

                } else {
                    updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", "PINSTID/FOLDERID IS NULL");
                }

                //}
//                    } else {
//
//                        updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "SKIP", "");
//
//                    }
//                } else {
//                    updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "SKIP", "");
//                    updateDATA(con, "Fail", dataMap);
//                    System.out.println("[MF-SFTP]--LOANTYPE_IS NULL");
//
//                }
            }
            closeCallableConnection(rs, callableStatement);
        } catch (Exception ex) {
            //updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            //System.out.println("[MF-SFTP]-- UPDATED MF_DOCEXE_SFTP: EXCEPTION OCCURED FOR:" + dataMap.get("PROCESSINSTANCEID").toString());

            ex.getCause();
            System.out.println("[MF-SFTP]--Inside Exception - " + ex);
        } finally {
            closeCallableConnection(rs, callableStatement);
        }
    }

    public boolean updateDATA(Connection con, String type, Map<String, String> Datamap) {
        String query = "";
        PreparedStatement pstmt = null;
        try {
            if (type.equalsIgnoreCase("Fail")) {
                query = "Insert Into MF_SFTP_FAIL (Pinstid,Cifid,Found_M_Docs,Sftp_Upload_Status,REMARKS,Executed_Time) VALUES(?,?,?,?,?,SYSDATE)";
                pstmt.setString(1, Datamap.get("PROCESSINSTANCEID"));
                pstmt.setString(2, Datamap.get("CIFID"));
                pstmt.setString(3, "NO");
                pstmt.setString(4, "FAIL");
                pstmt.setString(5, "LOAN_TYPE IS NULL");
                pstmt.setString(6, "FAIL");
            }
            pstmt.execute();
            closeConnection(null, pstmt);
        } catch (SQLException ex) {
            System.out.println("[DOC Upload] -- UPdate Status Eex - " + ex);
            return Boolean.FALSE;
        } finally {
            closeConnection(null, pstmt);
        }
        return Boolean.TRUE;
    }

    public int initiateDocUpload(Map dataMap, Long folderId, Connection con, String pinstid) {

        //CH-D-2002
        Map Documents = new HashMap();
        //End
        ByteArrayInputStream is;
        int status = 0;
        try {
            DocumentRemoteModule documentModule = (DocumentRemoteModule) EJbContext.getContext("DocumentModuleServiceImpl", "SRV-DMS-APP", configObj.getDmsIp(), configObj.getDmsIp());
            System.out.println("[MERGE   DOC Upload] - DMS Ejb Object Created.");
            NodeFolder nf = new NodeFolder();
            nf.setUuid(folderId);
            System.out.println("[MF-SFTP]-- - sessionId-" + sessionId);

            tokenMap = checkDMSSession(con, status, configObj);
            if (tokenMap.get("status").equals("1")) {
                sessionId = tokenMap.get("sessionId").toString();
            }

            if (!sessionId.equals("")) {
                List<NodeDocument> nodeDoc = documentModule.getDocumentList(sessionId, nf);
                System.out.println("[MERGE  DOC Upolad] - Document Details Fetched.");
                if (!nodeDoc.isEmpty()) {

                    for (NodeDocument nodeD : nodeDoc) {
                        System.out.println("[MERGE DOC Upload] For Loop Started");
                        System.out.println(nodeD.getUuid());
                        System.out.println(nodeD.getName());
                        dataMap.put("DocName", nodeD.getName());
                        dataMap.put("EXT", nodeD.getExt());
                        Long versn = Long.valueOf(nodeD.getCurrentVersion().getVersion());
                        System.out.println("[MF-SFTP]-- - Doc Name:" + nodeD.getName());

                        System.out.println("[MF-SFTP]-- - Doc UUID:" + nodeD.getUuid());

                        if (nodeD.getName().equalsIgnoreCase("Group Photo.") || nodeD.getName().equalsIgnoreCase("Group Photo")) {
                            System.out.println("File Skipped");
                        } else {
                            is = new ByteArrayInputStream(documentModule.getDocumentContent(sessionId, nodeD.getUuid(), versn));
                            System.out.println("[MF-SFTP]-- - Going to extract Docs.");
                            //CH-D-2001
                            String docExtName = dataMap.get("DocName").toString();
                            System.out.println("[MF-SFTP]-- - docExtName---" + docExtName);

                            String DocTypeName = docExtName;
                            dataMap.put("ProofName", docExtName);
                            dataMap.put("CsvName", DocTypeName);

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
                            status = fileOperation(is, dataMap);

                            //END
                            is.close();
//                        if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "Copy", dataMap.get("TEMPLOC").toString())) {
//                            status = 1;
//                        }
                        } // skip close

                    }
                    System.out.println("[MERGE DOC Upload] For Loop Stopped");
                    System.out.println("[MF-SFTP]-- -- All Doc Extracted Initiating C-Kyc");

                    try {
                        //CH-D-2001
                        //cKyc(dataMap);
                        //cKyc(dataMap, con, pinstid);
                        cKycupdate(dataMap, con, pinstid);
                        System.out.println("[MERGE DOC Upload] CKYC Update Done");
                        //end
                    } catch (Exception ex) {

                    }
                }

            } else {
                System.out.println("[MF-SFTP]-- - Session Not Found To Upload Docs");
            }
        } catch (InvalidRepositoryException | PathNotFoundException | IOException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            System.out.println("[MF-SFTP]-- - EX -" + ex);
            if (ex.getMessage().contains("com.servo.dms.exception.RepositoryException: File Not Found for")) {
                errorFlag = "DONE";
            }
            return status;
        }
        return status;
    }

    public int fileOperation(ByteArrayInputStream is, Map dataMap) {
        String cykc_filename = null;
        CSVWriter writer = null;
        int status = 0;
        try {
            String docName = "";
            String extension = "";
            File file = new File("DMS_ARCHIVAL");
            if (!file.exists()) {
                boolean b = file.mkdirs();
                System.out.println("[MF-SFTP]-- - Base Directory Created.");
            }

            file = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            dataMap.put("TEMPLOC", configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            if (!file.exists()) {
                file.mkdir();
                System.out.println("[MF-SFTP]-- - Child Folder Created.");
                writer = new CSVWriter(new FileWriter(csv), '|', CSVWriter.NO_QUOTE_CHARACTER);
                String[] header = "FILENAME,DC.WINAME,DC.ACCNO,DC.CIFNO,DOCTYPE".split(",");
                writer.writeNext(header);
                writer.close();
            }

            File csvFile = new File(file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv");
            if (csvFile.exists()) {
                boolean bval = file.setExecutable(true);
                System.out.println("set the owner's execute permission: " + bval);
            } else {
                System.out.println("File cannot exists: ");
            }

            docName = dataMap.get("DocName").toString();
            extension = dataMap.get("EXT").toString();
            System.out.println("[MF-SFTP]---- Extension found- " + extension);
            System.out.println("[MF-SFTP]---- DocumentName- " + docName);
            //CH-D-2001
            String docExtName = dataMap.get("ProofName").toString();
            String DocTypeName = dataMap.get("CsvName").toString();
            System.out.println("[MF-SFTP]---- File opertaion docExtName- " + docExtName);
            System.out.println("[MF-SFTP]---- File opertaion docExtName- " + DocTypeName);

            if (docExtName.equalsIgnoreCase("Customer Photo")) {
                cykc_filename = "Photograph";
            } else if (docExtName.equalsIgnoreCase("PAN")) {
                cykc_filename = "Pancard";
            } else if (docExtName.equalsIgnoreCase("Signature")) {
                cykc_filename = "Signature";
            } else if (docExtName.equalsIgnoreCase("FORM60")) {
                cykc_filename = "Form60";
            } else {
                cykc_filename = docExtName;
            }

            //END
            if (extension.equalsIgnoreCase("pdf")) {
                byte[] array = new byte[is.available()];
                is.read(array);
                //END
                try ( //CH-D-2001
                        // FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension);
                        FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension)) {
                    //END
                    fos.write(array);
                }
                File pdffile = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension);
                if (pdffile.exists()) {
                    boolean bval = file.setExecutable(true);
                    System.out.println("set the owner's execute permission: " + bval);
                } else {
                    System.out.println("File cannot exists: ");
                }

            } else {
                BufferedImage bImageFromConvert = ImageIO.read(is);
                //CH-D-2001
                //ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension));
                ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension));
                //END

                File Imgfile = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension);
                if (Imgfile.exists()) {
                    boolean bval = file.setExecutable(true);
                    System.out.println("set the owner's execute permission: " + bval);
                } else {
                    System.out.println("File cannot exists: ");
                }

            }
            System.out.println("[MERGE DOC Upload] - Doc Copied To Server Locations--" + DocTypeName);

//            if (!DocTypeName.trim().startsWith("Address Proof Corress") && !DocTypeName.trim().startsWith("Relation ID Proof") && !DocTypeName.trim().startsWith("Second ID Proof") && !DocTypeName.trim().startsWith("ID Proof") && !DocTypeName.trim().startsWith("Address Proof")) {
//                System.out.println("[MERGE DOC Upload] - Inside the if loop" + DocTypeName);
//                String entry = docExtName + "." + extension + "|" + dataMap.get("PROCESSINSTANCEID") + "|" + dataMap.get("ACCOUNTNO") + "|" + dataMap.get("CIFID") + "|" + DocTypeName;
//                if (!checkDuplicateCsvEntry(csv, entry)) {
//                    System.out.println("[MERGE DOC Upload] - Inside DOC entry in CSV ");
//                    writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
//                    //CH-D-2001
//
//                    //String[] values = (docName + "." + extension + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + docName).split(",");
//                    String[] values = (docExtName + "." + extension + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + DocTypeName).split(",");
//                    //END
//                    writer.writeNext(values);
//                    writer.close();
//                }
//            }
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

            System.out.println("[MF MERGE DOC Upload] - Csv Entry Created");
            status = 1;

        } catch (IOException ex) {

            System.out.println("[MF-SFTP]-- - File writing exe - " + ex);
            ex.printStackTrace();
            return status;
        }
        return status;
    }

    public void cKycupdate(Map dataMap, Connection con, String pinstid) {
        CSVWriter writer = null;
        int status = 0;
        String fileA = "";
        File testFile = null;
        String fileB = "";
//        String path = System.getenv("DOMAIN_HOME") + File.separator + "DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");

//PROD
//String path = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        String path = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        try {
            //MY CODE

            Map HashCustProof = getDocuemntsDetails(con, pinstid);
            System.out.println("[MERGE DOC Upload] - HashCustProof---" + HashCustProof.toString());
            String IdProoftype = HashCustProof.get("IDPROOFTYPE").toString();
            System.out.println("[MERGE DOC Upload] - IdProoftype---" + IdProoftype);
            String AddressProofType = HashCustProof.get("ADDRESS_PROOF_TYPE").toString();
            System.out.println("[MERGE DOC Upload] - IdProoftype---" + AddressProofType);

            System.out.println("[MERGE DOC Upload] -- Completed the CKYC Documents Chages");

            //end
            File file = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[MERGE Doc Upload] - CKyc-- File-" + file.getAbsolutePath());
            configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[MERGE Doc Upload] - TEMPLOC-- SFTP-" + configObj.getDmsSftpLocalDir());
            dataMap.put("TEMPLOC", configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
            System.out.println("[MERGE Doc Upload] - CKyc-- csv file-" + csv);
            List<String> files = new ArrayList<>();
            String StrVal = "Address Proof,ID Proof,Address proof Corress,Relation ID Proof,Second ID Proof";
            String[] arrSplit = StrVal.split(",");
            String StatusValue = "S";
            String csvName = "";
            String proofName = "";
            for (String strTemp : arrSplit) {
                System.out.println("1--Working==" + strTemp);
                System.out.println("Status---" + StatusValue);
                if ((IdProoftype.equalsIgnoreCase("1")) && (AddressProofType.equalsIgnoreCase("1")) && StatusValue.equalsIgnoreCase("S")) {

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

                        proofName = "UID.pdf";

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
                    StatusValue = "N";
                } else if (strTemp.equalsIgnoreCase("Address proof Corress") || strTemp.equalsIgnoreCase("Relation ID Proof") || strTemp.equalsIgnoreCase("Second ID Proof")) {
                    System.out.println("5--Working==");
                    fileA = path + File.separator + strTemp + " Front.jpg";
                    fileB = path + File.separator + strTemp + " Back.jpg";
                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    proofName = strTemp + ".pdf";
                    StatusValue = "Y";
                }
                testFile = new File(fileA);

                if (testFile.exists()) {
                    files.add(fileA);
                    files.add(fileB);
                    //ImageToPDF(files, path + File.separator + "Address Proof.pdf");
                    System.out.println("-fileA--" + fileA);
                    System.out.println("-fileA--" + fileB);
                    System.out.println("Pdf uploda csvname" + csvName);
                    ImageToPDF(files, path + File.separator + proofName);
                    String[] values = (proofName + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + csvName).split(",");
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
                    System.out.println("[MERGE Doc Upload] - MERGE -- Doc Not Found-Address Proof");
                }

                // if (StatusValue.equalsIgnoreCase("Y")) {
                //     break;
                //}
            }
            writer.close();

        } catch (IOException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            System.out.println("[MF-SFTP]-- -  Ex- " + ex);
            ex.printStackTrace();
        }
    }

    public void cKyc(Map dataMap, Connection con, String pinstid) {
        CSVWriter writer = null;
        int status = 0;
        String fileA = "";
        File testFile = null;
        String fileB = "";
//        String path = System.getenv("DOMAIN_HOME") + File.separator + "DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        String path = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        try {
            //MY CODE

            Map HashCustProof = getDocuemntsDetails(con, pinstid);
            System.out.println("[MF-SFTP]-- - HashCustProof---" + HashCustProof.toString());
            String IdProoftype = HashCustProof.get("IDPROOFTYPE").toString();
            System.out.println("[MF-SFTP]-- - IdProoftype---" + IdProoftype);
            String AddressProofType = HashCustProof.get("ADDRESS_PROOF_TYPE").toString();
            System.out.println("[MF-SFTP]-- - IdProoftype---" + AddressProofType);

            System.out.println("[MF-SFTP]-- -- Completed the CKYC Documents Chages");

            //end
            File file = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[MF-SFTP]-- - CKyc-- File-" + file.getAbsolutePath());
            configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[CKYC Doc Upload] - TEMPLOC-- SFTP-" + configObj.getDmsSftpLocalDir());
            dataMap.put("TEMPLOC", configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
            System.out.println("[MF-SFTP]-- - CKyc-- csv file-" + csv);
            List<String> files = new ArrayList<String>();
            String StrVal = "Address Proof,ID Proof";
            String[] arrSplit = StrVal.split(",");
            String StatusValue = "Y";
            String csvName = "";
            String proofName = "";
            for (String strTemp : arrSplit) {
                if (((IdProoftype.equalsIgnoreCase("1")) && (AddressProofType.equalsIgnoreCase("1")))) {

                    fileA = path + File.separator + "Address Proof Front.jpg";
                    fileB = path + File.separator + "Address Proof Back.jpg";
                    csvName = "Address_ID Proof";

                    proofName = "UID.pdf";
                    StatusValue = "Y";
                } else if (strTemp.equalsIgnoreCase("Address Proof")) {

                    fileA = path + File.separator + "Address Proof Front.jpg";
                    fileB = path + File.separator + "Address Proof Back.jpg";
                    csvName = strTemp;
                    StatusValue = "N";
                    if ((AddressProofType.equalsIgnoreCase("1"))) {

                        proofName = "UID.pdf";

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
                } else if (strTemp.equalsIgnoreCase("ID Proof")) {

                    fileA = path + File.separator + "ID Proof Front.jpg";
                    fileB = path + File.separator + "ID Proof Back.jpg";
                    csvName = strTemp;
                    StatusValue = "N";
                    if ((IdProoftype.equalsIgnoreCase("1"))) {

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
                }
                testFile = new File(fileA);

                if (testFile.exists()) {
                    files.add(fileA);
                    files.add(fileB);
                    //ImageToPDF(files, path + File.separator + "Address Proof.pdf");
                    ImageToPDF(files, path + File.separator + proofName);
                    String[] values = (proofName + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + csvName).split(",");
                    writer.writeNext(values);

                    files.clear();
                } else {
                    System.out.println("[MF-SFTP]-- - CKyc-- Doc Not Found-Address Proof");
                }

                if (StatusValue.equalsIgnoreCase("Y")) {
                    break;
                }
            }
            writer.close();

        } catch (IOException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            System.out.println("[MF-SFTP]-- - CKyc Ex- " + ex);
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
            System.out.println("[MF-SFTP]-- - C Kyc Doc Write Exc- " + e);
        }
    }

    public void initiateSftp(propertyConfig configObj, Connection con, Map dataMap) {
        String remoteDirectory;
        StandardFileSystemManager manager = new StandardFileSystemManager();
        String serverAddress = configObj.getDmsSftpIp();
        String userId = configObj.getDmsSftpUserId();
        String password = configObj.getDmsSftpPswd();
        //String remoteDirectory = configObj.getDmsSftpDir();

        System.out.println("[MF SFTP IP--" + serverAddress);
        System.out.println("[MF SFTP USERID--" + userId);
        System.out.println("[MF SFTP PASSWORD--" + password);

        //PROD
        remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/IN24/";
        //UAT
        //remoteDirectory = "BPMDOCUPLOAD/IN/";
        System.out.println("[MF SFTP REMOTE DIRECTORY]--" + remoteDirectory);
        //String localDirectory = "DMS_ARCHIVAL/NEW/";

        String localDirectory = "DMS_ARCHIVAL/MF/DOCEXE_SFTP/";
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
            System.out.println("[MF MERGE  DOC Upload] - Local Dir -" + localDirectory + fileToFTP);

            System.out.println("[MF MERGE  DOC Upload] - check if the file exists");
            String filepath = localDirectory + fileToFTP;
            File file = new File(filepath);
            if (!file.exists()) {
                throw new RuntimeException("MF MERGE  Error. Local file not found");
            }

            System.out.println("[MF MERGE  DOC Upload] - Initializes the file manager");
            manager.init();

            System.out.println("[MF MERGE  DOC Upload] - Setup our SFTP configuration");
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(
                    opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

            password = URLEncoder.encode(password);

            System.out.println("[MF MERGE  DOC Upload] - Create the SFTP URI using the host name, userid, password,  remote path and file name");
            String sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                    + remoteDirectory + fileToFTP;

            System.out.println("[MF MERGE  DOC Upload] -- " + sftpUri);

            System.out.println("[MF MERGE  DOC Upload] - Create local file object");
            FileObject localFile = manager.resolveFile(file.getAbsolutePath());

            System.out.println("[MF MERGE  DOC Upload] - Create remote file object");
            FileObject remoteFile = manager.resolveFile(sftpUri, opts);

            System.out.println("[MF MERGE  DOC Upload] -- Copy local file to sftp server");
            remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
            System.out.println("[MF MERGE  DOC Upload] -- Folder upload successful");

            File localDir = new File(localDirectory + fileToFTP);
            File[] subFiles = localDir.listFiles();
            for (File item : subFiles) {
                sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                        + remoteDirectory + fileToFTP + File.separator + item.getName();

                file = new File(localDirectory + fileToFTP + File.separator + item.getName());
                System.out.println("[MF MERGE  DOC Upload] -- " + file.getAbsolutePath());
                localFile = manager.resolveFile(file.getAbsolutePath());
                remoteFile = manager.resolveFile(sftpUri, opts);
                if (item.isFile()) {
                    remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                    System.out.println("MF remoteDirectory:" + remoteDirectory);
                    System.out.println("MF remoteDirectory remain:" + fileToFTP + File.separator + item.getName());
                    System.out.println("MF remoteDirectory fileToFTP:" + fileToFTP);
                    System.out.println("MF remoteDirectory item :" + item.getName());
                    cSftp.chmod(511, remoteDirectory + fileToFTP + "/" + item.getName());
                    System.out.println("[MF MERGE  DOC Upload] -- File upload successful");
                }
            }
            cSftp.chmod(511, remoteDirectory + "/" + dataMap.get("TEMPDOC_LOCATION").toString());
            System.out.println("[MF MERGE  DOC Upload] -- Updating Flag.");
            if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "Upload", "")) {

                configObj.setActivityId(Integer.parseInt(dataMap.get("ACTIVITYID").toString()));
                //RoutingResponse routeResponse = obj1.submitPInstance(con, dataMap.get("PROCESSINSTANCEID").toString(), configObj);
                //if (routeResponse.getStatus() == 1) {
                System.out.println("[MF MERGE DOC UPLOAD] -- Routing Done for - " + dataMap.get("PROCESSINSTANCEID").toString());
//                    File fileBkp = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
//                    removeDirectory(fileBkp);

                String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "DOCEXE_SFTP";
                String folderToDelete = "EXT-" + deletecustomerID;

                File directoryToDelete = new File(deletedirectoryPath, folderToDelete);

                if (directoryToDelete.exists()) {

                    System.out.println("[MF-SFTP]--Directory Found -- Deleting Existing Folder After upload to sftp: " + "EXT-" + deletecustomerID);
                    // Delete the directory
                    deleteDirectory(directoryToDelete);
                } else {
                    // Directory doesn't exist
                    System.out.println("[MF-SFTP]--Directory does not exist.");
                }
                //}
//                }
            }
        } catch (JSchException | SftpException | RuntimeException | FileSystemException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            System.out.println("[MF-SFTP]-- --File upload Ex - " + ex);
        } finally {
            // CH-D-2001
            manager.close();
            channel.disconnect();
            session.disconnect();

            //end
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
                query = "UPDATE MF_EXT SET TEMPDOC_FLAG = ?,TEMPDOC_LOCATION = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "DONE");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);
            } else if (type.equalsIgnoreCase("Upload")) {
                query = "UPDATE MF_EXT SET UPLOADDOC_FLAG = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "DONE");
                pstmt.setString(2, pinstId);
            } else if (type.equalsIgnoreCase("BYPASS")) {
                query = "UPDATE MF_EXT SET UPLOADDOC_FLAG = ? ,  DOCPURGEFLAG='O' WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "BYPASS");
                pstmt.setString(2, pinstId);

            } else if (type.equalsIgnoreCase("SKIP")) {
                query = "UPDATE MF_DOCEXE_SFTP SET EXECUTION_FLAG = ? ,REMARKS=? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "BYPASS");
                pstmt.setString(2, "SKIPPED");
                pstmt.setString(3, pinstId);

            } else if (type.equalsIgnoreCase("EXC")) {
                query = "UPDATE MF_DOCEXE_SFTP SET EXECUTION_FLAG = ? ,REMARKS=? WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "EXC");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);

            }
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
            query = "SELECT IDPROOFTYPE,ADDRESS_PROOF_TYPE FROM MF_EXT WHERE PINSTID=?";
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

    private boolean checkDuplicateCsvEntry(String csvPath, String newLine) {
        try {
            BufferedReader file = new BufferedReader(new FileReader(csvPath));
            String line;
            System.out.println("newLine-- " + newLine);
            while ((line = file.readLine()) != null) {
                System.out.println("line = " + line);
                if (line.equalsIgnoreCase(newLine)) {
                    file.close();
                    return true;
                }
            }
            file.close();

        } catch (Exception e) {
            System.out.println("checkDuplicateCsvEntry = " + e);
            e.printStackTrace();
        }
        return false;
    }
}
