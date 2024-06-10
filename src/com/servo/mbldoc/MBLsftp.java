//FILE_NAME   : MBLsftp.java
//AUTHOR NAME : Vinith
//
//    
//   SL-NO       CHANGE_ID          DATE                     DISCRIPTION  
//   ------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
//    1           CH-D-2003       01-03-2024                  * Renaming Docs in SFTP 
package com.servo.mbldoc;

//import com.itextpdf.text.Document;
//import com.itextpdf.text.Image;
//import com.itextpdf.text.Rectangle;
//import com.itextpdf.text.pdf.PdfStream;
//import com.itextpdf.text.pdf.PdfWriter;
import com.servo.businessdoc.SessionService;
import com.servo.configdoc.EJbContext;
import com.servo.configdoc.LoadConfig;
import com.servo.configdoc.propertyConfig;
import au.com.bytecode.opencsv.CSVWriter;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfWriter;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.servo.config.SRVTimerServiceConfig;
import com.servo.dms.entity.NodeDocument;
import com.servo.dms.entity.NodeFolder;
import com.servo.dms.module.remote.DocumentRemoteModule;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import static org.apache.commons.io.FilenameUtils.removeExtension;

@TimerService
public class MBLsftp {

    propertyConfig configObj = new propertyConfig();
    SessionService service = new SessionService();
    HashMap tokenMap = new HashMap();
    String sessionId = "";
    @Resource
    private SRVTimerServiceConfig Timer_Service_Id;
    //SubmitInstance obj1 = new SubmitInstance();
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

    @Execute
    public void execute(Connection con) throws Exception {

        String missingDocumentsString = "";

        int sftpstatus = 0;
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long folderId = 0;
        String pinstid = null;
        Map dataMap = new HashMap();
        CallableStatement callableStatement = null;
        String getDMSUploadSql = "{call MBLUPLOADTOSFTP(?,?)}";

        try {

            callableStatement = con.prepareCall(getDMSUploadSql);
            callableStatement.setString(1, "1");
            callableStatement.registerOutParameter(2, OracleTypes.CURSOR);
            callableStatement.executeUpdate();
            rs = (ResultSet) callableStatement.getObject(2);

            while (rs.next()) {

                List<String> missingDocuments = new ArrayList<>();

//            List<String> foundDocument = new ArrayList<>();
                List<String> size_zero = new ArrayList<>();

                folderId = rs.getLong("FOLDERID");
                pinstid = rs.getString("PROCESSINSTANCEID");
                System.out.println("MBL DOC_EXE DOC UPLOAD - PROCESSINSTANCEID=" + pinstid);

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
                System.out.println("[MBL-SFTP]-- TEMPFLAG--FOR-" + pinstid + "--" + dataMap.get("TEMPFLAG").toString());
                System.out.println("[MBL-SFTP]-- UPLOADFLAG--FOR-" + pinstid + "--" + dataMap.get("UPLOADFLAG").toString());
                System.out.println("[MBL-SFTP]-- TEMPDOC_LOCATION--FOR-" + pinstid + "--" + dataMap.get("TEMPDOC_LOCATION").toString());
                if (pinstid != null && folderId != 0) {

                    String MBL_pinstid = null;

                    String MBL_cif = null;

                    String sftpdocs = null;

                    System.out.println("[MBL-SFTP]-- Data Fetched Going to initiate Doc Upload");
                    if (dataMap.get("TEMPFLAG").toString().equals("")) {
                        String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                        String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP";
                        String folderToDelete = "EXT-" + deletecustomerID;

                        File directoryToDelete = new File(deletedirectoryPath, folderToDelete);

                        if (directoryToDelete.exists()) {

                            System.out.println("[MBL-SFTP]--Directory Found -- Deleting Existing Folder before New file creation: " + "EXT-" + deletecustomerID);
                            // Delete the directory
                            deleteDirectory(directoryToDelete);
                        } else {
                            // Directory doesn't exist
                            System.out.println("[MBL-SFTP]--Directory does not exist.");
                        }

                        int Status = initiateDocUpload(dataMap, folderId, con, pinstid);

                        if (Status == 1) {
                            try {

                                Set<String> foundDocument = new HashSet<>();

                                //File directory = new File(directoryPath);
                                String customerID = dataMap.get("CUSTOMERID").toString();
                                String directoryPath = "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + customerID;
                                File directory = new File(directoryPath);

                                try {
                                    MBL_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
                                    System.out.println("[MBL-SFTP]-- SELECTING ID PROOF TYPE FOR PINSTID: " + MBL_pinstid);
                                    String idq = "SELECT PRIMARY_ID_TYPE AS ID_PROOF_TYPE FROM MBL_EXT WHERE PINSTID = ?";
                                    try (PreparedStatement pid = con.prepareStatement(idq)) {
                                        pid.setString(1, MBL_pinstid);
                                        try (ResultSet idrs = pid.executeQuery()) {
                                            while (idrs.next()) {
                                                String ID_PROOF_NUMBER = String.valueOf(idrs.getString("ID_PROOF_TYPE"));
                                                System.out.println("[MBL-SFTP]-- ID_PROOF_NUMBER: " + ID_PROOF_NUMBER);
                                                dataMap.put("ID_PROOF_NUMBER", ID_PROOF_NUMBER);
                                            }
                                        }
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                                String idprooftype = dataMap.get("ID_PROOF_NUMBER").toString();
                                System.out.println("[MBL-SFTP]-- PRIMARYIDPROOF_TYPE: " + idprooftype);
                                String[] documentNames = new String[10];
                                switch (idprooftype) {
                                    case "1":
                                        System.out.println("[MBL-SFTP]-- PRIMARY ID IS AADHAR");
                                        documentNames = new String[]{"Photograph", "Signature", "UID"};
                                        break;
                                    case "2":
                                        System.out.println("[MBL-SFTP]-- PRIMARY ID IS VOTER");
                                        documentNames = new String[]{"Photograph", "Signature", "Voters_Identity_Card"};
                                        break;
                                    case "3":
                                        System.out.println("[MBL-SFTP]-- PRIMARY ID IS Pancard");
                                        documentNames = new String[]{"Photograph", "Signature", "Pancard"};
                                        break;
                                    case "4":
                                        System.out.println("[MBL-SFTP]-- PRIMARY ID IS Driving Licence");
                                        documentNames = new String[]{"Photograph", "Signature", "Driving_License"};
                                        break;
                                    case "6":
                                        System.out.println("[MBL-SFTP]-- PRIMARY ID IS Passport");
                                        documentNames = new String[]{"Photograph", "Signature", "Passport"};
                                        break;
                                    default:
                                        documentNames = new String[]{"Photograph", "Signature", "UID"};
                                        System.out.println("[MBL-SFTP]-- ID TYPE IS NOT IN THE LIST NOW WE CONSIDER PRIMARY ID IS AADHAR");
                                        break;
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

                                missingDocumentsString = String.join(", ", missingDocuments);

                                //System.out.println("The missing documents are: " + missingDocumentsString );
                                sftpdocs = String.join(", ", foundDocument);

                                System.out.println("[MF-SFTP]--String of found documents: " + sftpdocs);

// Print the names of missing documents
                                if (!missingDocuments.isEmpty()) {
                                    System.out.println("[MBL-SFTP]--The following documents are missing:");
                                    missingDocuments.forEach((missingDocument) -> {
                                        System.out.println(missingDocument);
                                    });
                                    try {

                                        System.out.println("[MF-SFTP]--SETTING THE sftpstatus TO 3");

                                        sftpstatus = 3;
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                } else {
                                    System.out.println("[MBL-SFTP]--All required documents exist in the folder.");
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
                                            System.out.println("[MBL-SFTP]--Directory is empty.");
                                        }
                                    } else {
                                        System.out.println("[MBL-SFTP]--Directory does not exist or is not a directory.");
                                    }
                                    sftpstatus = (size_zero.size() > 0) ? 2 : 1;
                                    //sftpstatus = 1;
                                    System.err.println("[MBL-SFTP]--sftpstatus: " + sftpstatus);
                                }

// Use the 'status' variable as needed (send it to another method, etc.)
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            System.err.println("[MBL-SFTP]-- sftpstatus for SFTP initiation: " + sftpstatus);
                            try {
                                MBL_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
                                MBL_cif = dataMap.get("CUSTOMERID").toString();
                                switch (sftpstatus) {

                                    case 1: {

                                        System.out.println("[MBL-SFTP]-- Proceding For Sftp.");
                                        initiateSftp(configObj, con, dataMap);

                                        String squery = "Insert Into MBL_SFTP_SUCCESS (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(squery)) {
                                            ipstmt.setString(1, MBL_pinstid);
                                            ipstmt.setString(2, MBL_cif);
                                            ipstmt.setString(3, "YES");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, "PASS");
                                            ipstmt.execute();
                                        }
                                        String extquery = "UPDATE MBL_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "PASS");
                                            extstmt.setString(2, "FILES MOVED TO SFTP");
                                            extstmt.setString(3, MBL_pinstid);
                                            extstmt.executeUpdate();
                                        }
                                        String uquery = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "UD");
                                            upstmt.setString(2, "FILES MOVED TO SFTP");
                                            upstmt.setString(3, MBL_pinstid);
                                            upstmt.executeUpdate();
                                        }
                                        break;
                                    }
                                    case 2: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);
                                        String iquery = "Insert Into MBL_SFTP_FAIL (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Missing_Docs,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(iquery)) {
                                            ipstmt.setString(1, MBL_pinstid);
                                            ipstmt.setString(2, MBL_cif);
                                            ipstmt.setString(3, "NO");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, size_zero.toString());
                                            ipstmt.setString(6, "FAIL");
                                            ipstmt.execute();
                                        }
                                        String uquery = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "0kb File found");
                                            upstmt.setString(3, MBL_pinstid);
                                            upstmt.executeUpdate();
                                        }

                                        String extquery = "UPDATE MBL_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, "0 Kb file detected");
                                            extstmt.setString(3, MBL_pinstid);
                                            extstmt.executeUpdate();
                                        }
                                        break;
                                    }
                                    case 3: {
                                        System.out.println("[MBL-SFTP]-- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);

                                        String iquery = "Insert Into MBL_SFTP_FAIL (Pinstid,Cifid,Found_M_Docs,DOCS_IN_SFTP,Missing_Docs,Sftp_Upload_Status,Executed_Time) VALUES(?,?,?,?,?,?,SYSDATE)";
                                        try (PreparedStatement ipstmt = con.prepareStatement(iquery)) {
                                            ipstmt.setString(1, MBL_pinstid);
                                            ipstmt.setString(2, MBL_cif);
                                            ipstmt.setString(3, "NO");
                                            ipstmt.setString(4, sftpdocs);
                                            ipstmt.setString(5, missingDocumentsString);
                                            ipstmt.setString(6, "FAIL");
                                            ipstmt.execute();
                                        }

                                        String uquery = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "MISS");
                                            upstmt.setString(2, "FILES NOT FOUND");
                                            upstmt.setString(3, MBL_pinstid);
                                            upstmt.executeUpdate();
                                        }

                                        String extquery = "UPDATE MBL_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, missingDocumentsString + " Not FOUND");
                                            extstmt.setString(3, MBL_pinstid);
                                            extstmt.executeUpdate();
                                        }

                                        break;
                                    }
                                    default: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);
                                        String uquery = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "File Upload issue");
                                            upstmt.setString(3, MBL_pinstid);
                                            upstmt.executeUpdate();
                                            upstmt.execute();
                                        }
                                        break;
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        } else if (errorFlag.equalsIgnoreCase("DONE")) {
                            if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "")) {

                            }
                        }
                    }

                }
            }

            closeCallableConnection(rs, callableStatement);
        } catch (Exception ex) {
            System.out.println("CKYC  Inside Exception - " + ex);
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
            System.out.println("MBL DOC_EXE DOC UPLOAD - sessionId-" + sessionId);

            tokenMap = checkDMSSession(con, status, configObj);
            if (tokenMap.get("status").equals("1")) {
                sessionId = tokenMap.get("sessionId").toString();
            }

            if (!sessionId.equals("")) {
                List<NodeDocument> nodeDoc = documentModule.getDocumentList(sessionId, nf);
                System.out.println("[MBL-SFTP]-- Document Details Fetched.");
                if (!nodeDoc.isEmpty()) {

                    for (NodeDocument nodeD : nodeDoc) {
                        System.out.println("[MBL-SFTP]--NBS_UUID IN SDM_NODE_DOCUMENT: " + nodeD.getUuid());
                        System.out.println("[MBL-SFTP]--NBS_NAME IN SDM_NODE_DOCUMENT: " + nodeD.getName());
                        System.out.println("[MBL-SFTP]--EXTENSION IN SDM_NODE_DOCUMENT: " + nodeD.getExt());
                        dataMap.put("DocName", nodeD.getName());
                        dataMap.put("EXT", nodeD.getExt());
                        Long versn = Long.valueOf(nodeD.getCurrentVersion().getVersion());

                        if (nodeD.getName().equalsIgnoreCase("Group Photo.") || nodeD.getName().equalsIgnoreCase("Group Photo")) {
                            System.out.println("File Skipped");
                        } else {
                            is = new ByteArrayInputStream(documentModule.getDocumentContent(sessionId, nodeD.getUuid(), versn));
                            System.out.println("[MBL-SFTP]-- Going to extract Docs.");
                            //CH-D-2001
                            String docExtName = dataMap.get("DocName").toString();
                            System.out.println("[MBL-SFTP]-- docExtName---" + docExtName);

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
                    System.out.println("MBL DOC_EXE DOC UPLOAD -- All Doc Extracted Initiating C-Kyc");

                    try {
                        //CH-D-2001
                        //cKyc(dataMap);
                        //cKyc(dataMap, con, pinstid);
                        cKycupdate(dataMap, con, pinstid);
                        //end
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                } else if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "")) {
//                    configObj.setActivityId(Integer.parseInt(dataMap.get("ACTIVITYID").toString()));
//                    RoutingResponse routeResponse = obj1.submitPInstance(con, dataMap.get("PROCESSINSTANCEID").toString(), configObj);
//                    if (routeResponse.getStatus() == 1) {
//                        System.out.println("[DOC UPLOAD] -- Routing Done for - " + dataMap.get("PROCESSINSTANCEID").toString());
//                    }
                }

            } else {
                System.out.println("MBL DOC_EXE DOC UPLOAD - Session Not Found To Upload Docs");
            }
        } catch (Exception ex) {
            System.out.println("MBL DOC_EXE DOC UPLOAD - EX -" + ex);
            if (ex.getMessage().contains("com.servo.dms.exception.RepositoryException: File Not Found for")) {
                errorFlag = "DONE";
            }
            return status;
        }
        return status;
    }

    public void cKycupdate(Map<String, String> dataMap, Connection con, String pinstid) {
        CSVWriter writer = null;
        int status = 0;
        String fileA = "";
        File testFile = null;
        String fileB = "";
        String path = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        try {
            Map HashCustProof = getDocuemntsDetails(con, pinstid);
            System.out.println("[MBL-SFTP]-- HashCustProof---" + HashCustProof.toString());

            String IdProoftype = HashCustProof.get("IDPROOFTYPE").toString();
            System.out.println("[MBL-SFTP]-- IdProoftype---" + IdProoftype);

            String AddressProofType = HashCustProof.get("ADDRESS_PROOF_TYPE").toString();
            System.out.println("[MBL-SFTP]-- IdProoftype---" + AddressProofType);
            System.out.println("[MBL-SFTP]-- Completed the CKYC Documents Chages");

            File file = new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[MBL-SFTP]-- CKyc-- File-" + file.getAbsolutePath());

            this.configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[MBL-SFTP]-- TEMPLOC-- SFTP-" + this.configObj.getDmsSftpLocalDir());
            dataMap.put("TEMPLOC", this.configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
            System.out.println("[MBL-SFTP]-- CKyc-- csv file-" + csv);
            List<String> files = new ArrayList<>();

            String StrVal = "MBL Primary Id,MBL Corrs Id,MBL Bank Passbook";

            String[] arrSplit = StrVal.split(",");
            String StatusValue = "S";
            String csvName = "";
            String proofName = "";
            for (String strTemp : arrSplit) {
                System.out.println("[MBL-SFTP]--1--Working==" + strTemp);
                System.out.println("[MBL-SFTP]--Status---" + StatusValue);
                if (IdProoftype.equalsIgnoreCase("1") && AddressProofType.equalsIgnoreCase("1") && StatusValue.equalsIgnoreCase("S") && !dataMap.get("EKYCFLAG").toString().equalsIgnoreCase("Y")) {
                    fileA = path + File.separator + "MBL Primary Id Front.jpg";
                    fileB = path + File.separator + "MBL Primary Id BACK.jpg";
                    csvName = "Address_ID Proof";
                    proofName = "UID.pdf";
                    StatusValue = "Y";
                    System.out.println("2--Working==");
                } else if (strTemp.equalsIgnoreCase("FIG ADDRESS PROOF")) {
                    System.out.println("3--Working==");
                    fileA = path + File.separator + "FIG ADDRESS PROOF FRONT.jpg";
                    fileB = path + File.separator + "FIG ADDRESS PROOF BACK.jpg";
                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    if (AddressProofType.equalsIgnoreCase("1") && !StatusValue.equalsIgnoreCase("Y")) {
                        proofName = "UID.pdf";
                    } else if (AddressProofType.equalsIgnoreCase("2")) {
                        proofName = "Voters_Identity_Card.pdf";
                    } else if (AddressProofType.equalsIgnoreCase("4")) {
                        proofName = "Passport.pdf";
                    } else if (AddressProofType.equalsIgnoreCase("5")) {
                        proofName = "Utility_Bill.pdf";
                    } else if (AddressProofType.equalsIgnoreCase("6")) {
                        proofName = "Driving_License.pdf";
                    } else if (AddressProofType.equalsIgnoreCase("7")) {
                        proofName = "NREGA_Job_Card.pdf";
                    } else if (AddressProofType.equalsIgnoreCase("8")) {
                        proofName = "Utility_Bill.pdf";
                    } else if (AddressProofType.equalsIgnoreCase("3")) {
                        proofName = "Other.pdf";
                    }
                    StatusValue = "N";
                } else if (strTemp.equalsIgnoreCase("MBL Primary Id")) {
                    System.out.println("4--Working==");
                    fileA = path + File.separator + "MBL Primary Id Front.jpg";
                    fileB = path + File.separator + "MBL Primary Id Back.jpg";
                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    if (IdProoftype.equalsIgnoreCase("1") && !StatusValue.equalsIgnoreCase("Y")) {
                        proofName = "UID.pdf";
                    } else if (IdProoftype.equalsIgnoreCase("2")) {
                        proofName = "Voters_Identity_Card.pdf";
                    } else if (IdProoftype.equalsIgnoreCase("3")) {
                        proofName = "Pancard.pdf";
                    } else if (IdProoftype.equalsIgnoreCase("4")) {
                        proofName = "Driving_License.pdf";
                    } else if (IdProoftype.equalsIgnoreCase("5")) {
                        proofName = "Pension_Order.pdf";
                    } else if (IdProoftype.equalsIgnoreCase("6")) {
                        proofName = "Passport.pdf";
                    } else if (IdProoftype.equalsIgnoreCase("8")) {
                        proofName = "NREGA_Job_Card.pdf";
                    } else if (IdProoftype.equalsIgnoreCase("9")) {
                        proofName = "Bank_Statement.pdf";
                    }
                    StatusValue = "N";
                } else if (strTemp.equalsIgnoreCase("MBL Corrs Id")) {
                    System.out.println("5--Working==");

                    fileA = path + File.separator + strTemp + " Front.jpg";
                    fileB = path + File.separator + strTemp + " Back.jpg";

//                    switch (strTemp) {
//                        case "FIG RELATION ID": {
//                            fileA = path + File.separator + strTemp + " PROOF FRONT.jpg";
//                            fileB = path + File.separator + strTemp + " BACK.jpg";
//                            break;
//                        }
//                        default: {
//                            fileA = path + File.separator + strTemp + " Front.jpg";
//                            fileB = path + File.separator + strTemp + " Back.jpg";
//                            break;
//                        }
//                    }
                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    proofName = strTemp + ".pdf";
                    StatusValue = "Y";
                } else if (strTemp.equalsIgnoreCase("MBL Bank Passbook")) {
                    System.out.println("5--Working==");

                    fileA = path + File.separator + strTemp + " Image page1.jpg";
                    fileB = path + File.separator + strTemp + " Image page2.jpg";

                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    //proofName = strTemp + ".pdf";
                    proofName = "Bank_Statement.pdf";
                    StatusValue = "Y";
                }
                testFile = new File(fileA);
                if (testFile.exists()) {
                    files.add(fileA);
                    files.add(fileB);
                    System.out.println("-fileA--" + fileA);
                    System.out.println("-fileA--" + fileB);
                    System.out.println("Pdf upload csvname: " + csvName);
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
                    System.out.println("[MBL-SFTP]-- Doc Not Found-Address Proof");
                }
            }
            writer.close();
        } catch (FileNotFoundException fx) {
            System.out.println("[MBL-SFTP]--Updating File not found");
            updateFlag(con, dataMap.get("PROCESSINSTANCEID"), "ERR", "File is not created");
            System.out.println("[MBL-SFTP]--Updating File ");
        } catch (IOException ex) {
            System.out.println("[MBL-SFTP]-- CKyc Ex- " + ex);
            ex.printStackTrace();
        }
    }

    public static void ImageToPDF(List<String> inputFiles, String outputFile) {
        try {
            Document document = null;
            FileOutputStream fos = new FileOutputStream(outputFile);
            Image img = Image.getInstance(inputFiles.get(0));
            Rectangle rc = new Rectangle(0.0F, 0.0F, img.getWidth(), img.getHeight());
            img = null;
            document = new Document(rc);
            PdfWriter writer = PdfWriter.getInstance(document, fos);
            writer.setCompressionLevel(9);
            document.open();
            for (int i = 0; i < inputFiles.size(); i++) {
                img = Image.getInstance(inputFiles.get(i));
                rc = new Rectangle(0.0F, 0.0F, img.getWidth(), img.getHeight());
                document.setPageSize(rc);
                document.newPage();
                img.setAbsolutePosition(0.0F, 0.0F);
                document.add((Element) img);
                img = null;
            }
            document.close();
            writer.close();
            fos.close();
        } catch (DocumentException | IOException e) {
            System.out.println("[MBL-SFTP]-- - C Kyc Doc Write Exc- " + e);
        }
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
                System.out.println("MBL DOC_EXE DOC UPLOAD - Base Directory Created.");
            }

            System.out.println("---Absolute path-1-" + file.getAbsolutePath());

            String customerID = dataMap.get("CUSTOMERID").toString();
//            String directoryPath = "DMS_ARCHIVAL" + File.separator + "CKYC" + File.separator + "EXT-" + customerID;
//            File directory = new File(directoryPath);

            file = new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            //System.out.println("----"+);
            configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            dataMap.put("TEMPLOC", configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            System.out.println("---Absolute path-2---" + file.getAbsolutePath());
            if (!file.exists()) {
                file.mkdir();
                System.out.println("MBL DOC_EXE DOC UPLOAD - Child Folder Created.");
                writer = new CSVWriter(new FileWriter(csv), '|', CSVWriter.NO_QUOTE_CHARACTER);
                String[] header = "FILENAME,DC.WINAME,DC.ACCNO,DC.CIFNO,DOCTYPE".split(",");
                writer.writeNext(header);
                writer.close();
            }
            docName = dataMap.get("DocName").toString();
            extension = dataMap.get("EXT").toString();
            System.out.println("MBL DOC_EXE DOC UPLOAD-- Extension found- " + extension);
            System.out.println("MBL DOC_EXE DOC UPLOAD-- DocumentName- " + docName);
            //CH-D-2001
            String docExtName = dataMap.get("ProofName").toString();
            String DocTypeName = dataMap.get("CsvName").toString();
            System.out.println("MBL DOC_EXE DOC UPLOAD-- File opertaion docExtName- " + docExtName);
            System.out.println("MBL DOC_EXE DOC UPLOAD-- File opertaion docExtName- " + DocTypeName);

            if (docExtName.equalsIgnoreCase("MBL CUSTOMER PHOTO")) {
                cykc_filename = "Photograph";
            } else if (docExtName.equalsIgnoreCase("MBL PAN CARD")) {
                cykc_filename = "Pancard";
            } else if (docExtName.equalsIgnoreCase("MBL SIGNATURE OR THUMB")) {
                cykc_filename = "Signature";
            } else if (docExtName.equalsIgnoreCase("MBL FORM60")) {
                cykc_filename = "Form60";
            } else {
                cykc_filename = docExtName;
            }

            if (extension.equalsIgnoreCase("pdf")) {
                byte[] array = new byte[is.available()];
                is.read(array);
                try ( //CH-D-2001
                        // FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension);
                        FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension)) {
                    System.out.println("[MBL-SFTP]--CKYC UPLOAD FILE - DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension);
                    //END
                    fos.write(array);
                }
            } else {
                BufferedImage bImageFromConvert = ImageIO.read(is);
                //CH-D-2001
                //ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docName + "." + extension));
                ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension));
                System.out.println(("[MBL-SFTP]-- IMAGE - DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + cykc_filename + "." + extension));
//END
            }

            //END
            System.out.println("[MBL-SFTP]-- Doc Copied To Server Locations--" + DocTypeName);

            if (!cykc_filename.trim().startsWith("Address Proof Corress") && !cykc_filename.trim().startsWith("Relation ID Proof") && !cykc_filename.trim().startsWith("Second ID Proof") && !cykc_filename.trim().startsWith("ID Proof") && !cykc_filename.trim().startsWith("Address Proof")) {
                System.out.println("[MBL-SFTP]-- Inside the if loop: " + DocTypeName);
                writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
                //CH-D-2001

                //String[] values = (docName + "." + extension + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + docName).split(",");
                String[] values = (cykc_filename + "." + extension + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + cykc_filename).split(",");
                //END
                writer.writeNext(values);
                writer.close();
            }

            System.out.println("[MBL DOC Upload] - Csv Entry Created");

            status = 1;

        } catch (Exception ex) {
            System.out.println("MBL DOC_EXE DOC UPLOAD - File writing exe - " + ex);
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

    public void initiateSftp(propertyConfig configObj, Connection con, Map dataMap) throws SQLException {
        StandardFileSystemManager manager = new StandardFileSystemManager();

        String serverAddress = configObj.getDmsSftpIp();
        String userId = configObj.getDmsSftpUserId();
        String password = configObj.getDmsSftpPswd();

        //PROD DIRECTORY
        String remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/IN24/";
        //UAT DIRECTORY
//        String remoteDirectory = configObj.getDmsSftpDir();
        //String remoteDirectory = "SERVOSYS/ESAFUAT/BPMDOCUPLOAD/IN24/";
        //String password = "SRV#1NrtpJ$XcW6";
        //String remoteDirectory = "BPMDOCUPLOAD/IN/";
        System.out.println("DOC_EXE SFTP IP: " + serverAddress);
        System.out.println("DOC_EXE userId: " + userId);
        System.out.println("DOC_EXE password: " + password);
        System.out.println("[MBL CKYC DOC Upload Remote Dir]--" + remoteDirectory);
        String localDirectory = "DMS_ARCHIVAL/MBL/DOCEXE_SFTP/";
        System.out.println("[MBL CKYC DOC Upload LOCAL Dir]--" + localDirectory);
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

            System.out.println("MBL DOC_EXE DOC UPLOAD - Local Dir -" + localDirectory + fileToFTP);

            System.out.println("MBL DOC_EXE DOC UPLOAD - check if the file exists");
            String filepath = localDirectory + fileToFTP;
            File file = new File(filepath);
            if (!file.exists()) {
                throw new RuntimeException("CKYC Error. Local file not found");
            }

            System.out.println("MBL DOC_EXE DOC UPLOAD - Initializes the file manager");
            manager.init();

            System.out.println("MBL DOC_EXE DOC UPLOAD - Setup our SFTP configuration");
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(
                    opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

            password = URLEncoder.encode(password);

            System.out.println("MBL DOC_EXE DOC UPLOAD - Create the SFTP URI using the host name, userid, password,  remote path and file name");
            String sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                    + remoteDirectory + fileToFTP;

            System.out.println("MBL DOC_EXE DOC UPLOAD --" + sftpUri);

            System.out.println("MBL DOC_EXE DOC UPLOAD - Create local file object");
            FileObject localFile = manager.resolveFile(file.getAbsolutePath());

            System.out.println("MBL DOC_EXE DOC UPLOAD - Create remote file object");
            FileObject remoteFile = manager.resolveFile(sftpUri, opts);

            System.out.println("MBL DOC_EXE DOC UPLOAD -- Copy local file to sftp server");
            remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
            System.out.println("MBL DOC_EXE DOC UPLOAD -- Folder upload successful");

            File localDir = new File(localDirectory + fileToFTP);
            File[] subFiles = localDir.listFiles();
            for (File item : subFiles) {
                sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                        + remoteDirectory + fileToFTP + File.separator + item.getName();

                file = new File(localDirectory + fileToFTP + File.separator + item.getName());
                System.out.println("MBL DOC_EXE DOC UPLOAD -- " + file.getAbsolutePath());
                localFile = manager.resolveFile(file.getAbsolutePath());
                remoteFile = manager.resolveFile(sftpUri, opts);
                if (item.isFile()) {
                    remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                    cSftp.chmod(511, remoteDirectory + fileToFTP + "/" + item.getName());
                    System.out.println("MBL DOC_EXE DOC UPLOAD -- File upload successful");
                }
            }

            cSftp.chmod(511, remoteDirectory + "/" + dataMap.get("TEMPDOC_LOCATION").toString());
            System.out.println("MBL DOC_EXE DOC UPLOAD -- Updating Flag.");
            if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "")) {
//                configObj.setActivityId(Integer.parseInt(dataMap.get("ACTIVITYID").toString()));
//                RoutingResponse routeResponse = obj1.submitPInstance(con, dataMap.get("PROCESSINSTANCEID").toString(), configObj);
//                if (routeResponse.getStatus() == 1) {
                System.out.println("MBL DOC_EXE DOC UPLOAD -- Routing Done for - " + dataMap.get("PROCESSINSTANCEID").toString());
//                File fileBkp = new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
//                deleteDirectory(fileBkp);
                String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "DOCEXE_SFTP";
                String folderToDelete = "EXT-" + dataMap.get("CUSTOMERID");

                File directoryToDelete = new File(deletedirectoryPath, folderToDelete);

                if (directoryToDelete.exists()) {

                    System.out.println("Directory Found -- Deleting Folder After SFTP Updation: " + "EXT-" + dataMap.get("CUSTOMERID"));
                    // Delete the directory
                    deleteDirectory(directoryToDelete);
                } else {
                    // Directory doesn't exist
                    System.out.println("Directory does not exist.");
                }

//                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("MBL DOC_EXE DOC UPLOAD --File upload Ex - " + ex);
            System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
            String mbl_pinstid = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
            String uquery = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
            try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                upstmt.setString(1, "FAIL");
                upstmt.setString(2, "SFTP issue");
                upstmt.setString(3, mbl_pinstid);
                upstmt.executeUpdate();
                upstmt.execute();
            }

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
                query = "UPDATE MBL_EXT SET TEMPDOC_FLAG = ?,TEMPDOC_LOCATION = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "DONE");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);
            } else if (type.equalsIgnoreCase("Upload")) {
                query = "UPDATE MBL_EXT SET UPLOADDOC_FLAG = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "DONE");
                pstmt.setString(2, pinstId);

            } else if (type.equalsIgnoreCase("BYPASS")) {
                query = "UPDATE MBL_EXT SET UPLOADDOC_FLAG = ? WHERE PINSTID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "BYPASS");
                pstmt.setString(2, pinstId);
            } else if (type.equalsIgnoreCase("UPD")) {
                query = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ? ,SIG_PIC_UPTIME=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                //Changed By Anurag 02-01-2019 for changing the IN and response OUT23
                //pstmt.setString(1, "U");
                pstmt.setString(1, "UD");
                //End by Anurag
                pstmt.setString(2, pinstId);
            } else if (type.equalsIgnoreCase("CKYC_MISSING")) {
                query = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? ,SIG_PIC_UPTIME=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "MISS");
                pstmt.setString(2, "FILES NOT FOUND");
                pstmt.setString(3, pinstId);
            } else if (type.equalsIgnoreCase("ERR")) {
                query = "UPDATE MBL_DOCEXE_SFTP SET EXECUTION_FLAG = ?,REMARKS = ? ,SIG_PIC_UPTIME=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "E");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);
            }
            pstmt.executeUpdate();
            closeConnection(null, pstmt);
        } catch (Exception ex) {
            System.out.println("MBL DOC_EXE DOC UPLOAD -- UPdate Status Eex - " + ex);
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
            query = "SELECT PRIMARY_ID_TYPE,ADDRESS_PROOF_TYPE FROM MBL_EXT WHERE PINSTID =?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, strPinstid);
            rs = pstmt.executeQuery();

            while (rs.next()) {

                dataProof.put("IDPROOFTYPE", rs.getString("PRIMARY_ID_TYPE"));
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
