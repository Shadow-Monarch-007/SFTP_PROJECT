package com.servo.mbldoc;

import au.com.bytecode.opencsv.CSVWriter;
import com.itextpdf.text.Document;
import com.itextpdf.text.Element;
import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfWriter;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.servo.businessdoc.SessionService;
import com.servo.config.SRVTimerServiceConfig;
import com.servo.configdoc.EJbContext;
import com.servo.configdoc.LoadConfig;
import com.servo.configdoc.propertyConfig;
import com.servo.dms.entity.NodeDocument;
import com.servo.dms.entity.NodeFolder;
import com.servo.dms.entity.Repository;
import com.servo.dms.exception.InvalidRepositoryException;
import com.servo.dms.module.remote.DocumentRemoteModule;
import static com.servo.mbldoc.MBLsftp.ImageToPDF;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.Selectors;
import org.apache.commons.vfs.impl.StandardFileSystemManager;
import org.apache.commons.vfs.provider.sftp.SftpFileSystemConfigBuilder;

@TimerService
public class MBLCkycUpload {

    propertyConfig configObj = new propertyConfig();

    SessionService service = new SessionService();

    HashMap tokenMap = new HashMap<>();

    String sessionId = "";

    Set<String> uniqueEntries;

    @Resource
    private SRVTimerServiceConfig Timer_Service_Id;

    String errorFlag = "";

    @PostConstruct
    public void loadConfigurations(Connection con1) throws NoSuchProviderException, MessagingException {
        try {
            System.out.println("[init] - Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(this.configObj);
            System.out.println("SftpIp - " + this.configObj.getDmsSftpIp());
            System.out.println("SftpUserId - " + this.configObj.getDmsSftpUserId());
            System.out.println("SftpDir - " + this.configObj.getDmsSftpDir());
            this.tokenMap = this.service.getSession(this.configObj.getIstreamsIp(), this.configObj.getIstreamsPort(), "DMS_USER", "system123#");
            if (this.tokenMap.get("status").equals("1")) {
                this.sessionId = this.tokenMap.get("sessionId").toString();
            }
            System.out.println("[init] - Post construct Executed Config Loaded For Doc Upload.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[init] - Config Load Exception->" + ex);
        }
    }

    @Execute
    public void execute(Connection con) throws Exception {
        int sftpstatus = 0;
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long folderId = 0L;
        String pinstid = null;
        Map<Object, Object> dataMap = new HashMap<>();
        CallableStatement callableStatement = null;
        String getDMSUploadSql = "{call getMBLCKYCUploadCase(?,?)}";
        try {
            callableStatement = con.prepareCall(getDMSUploadSql);
            callableStatement.setString(1, "1");
            callableStatement.registerOutParameter(2, -10);
            callableStatement.executeUpdate();
            rs = (ResultSet) callableStatement.getObject(2);
            while (rs.next()) {
                String missingDocumentsString = "";
                List<String> missingDocuments = new ArrayList<>();
                List<String> size_zero = new ArrayList<>();
                uniqueEntries = new HashSet<>();
                System.out.println("[MBLCKYC-UPLOAD]-- PROCESSINSTANCEID=" + pinstid);
                folderId = rs.getLong("FOLDERID");
                pinstid = rs.getString("PROCESSINSTANCEID");
                dataMap.put("PROCESSINSTANCEID", pinstid);
                dataMap.put("FOLDERID", folderId);
                dataMap.put("ACTIVITYID", "NA");
                dataMap.put("ACCOUNTNO", rs.getString("ACCOUNTNO"));
                dataMap.put("CIFID", rs.getString("CIFID"));
                dataMap.put("CUSTOMERID", rs.getString("CIFID"));
                dataMap.put("EKYCFLAG", rs.getString("EKYCFLAG"));
                dataMap.put("TEMPFLAG", (rs.getString("TEMPDOC_FLAG") == null) ? "" : rs.getString("TEMPDOC_FLAG"));
                dataMap.put("UPLOADFLAG", (rs.getString("UPLOADDOC_FLAG") == null) ? "" : rs.getString("UPLOADDOC_FLAG"));
                dataMap.put("TEMPDOC_LOCATION", (rs.getString("TEMPDOC_LOCATION") == null) ? "" : rs.getString("TEMPDOC_LOCATION"));
                System.out.println("[MBLCKYC-UPLOAD]-- TEMPFLAG--FOR-" + pinstid + "--" + dataMap.get("TEMPFLAG").toString());
                System.out.println("[MBLCKYC-UPLOAD]-- UPLOADFLAG--FOR-" + pinstid + "--" + dataMap.get("UPLOADFLAG").toString());
                System.out.println("[MBLCKYC-UPLOAD]-- TEMPDOC_LOCATION--FOR-" + pinstid + "--" + dataMap.get("TEMPDOC_LOCATION").toString());
                if (pinstid != null && folderId != 0L) {
                    System.out.println("[MBLCKYC-UPLOAD]-- Data Fetched Going to initiate Doc Upload");
                    String MBL_PINSTID = null;
                    String MBL_CIF = null;
                    String sftpdocs = null;
                    if (dataMap.get("TEMPFLAG").toString().equals("")) {
                        String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                        String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC";
                        String folderToDelete = "EXT-" + deletecustomerID;
                        File directoryToDelete = new File(deletedirectoryPath, folderToDelete);
                        if (directoryToDelete.exists()) {
                            System.out.println("Directory Found -- Deleting Existing Folder before New file creation: EXT-" + deletecustomerID);
                            deleteDirectory(directoryToDelete);
                        } else {
                            System.out.println("Directory does not exist.");
                        }
                        //int Status = initiateDocUpload(dataMap, Long.valueOf(folderId), con, pinstid);
                        int Status = initiateDocUpload(dataMap, folderId, con, pinstid);
                        System.out.println("[MBLCKYC-UPLOAD]-- Status:" + Status);
                        if (Status == 1) {
                            try {
                                Set<String> foundDocument = new HashSet<>();
                                String idtype = null;
                                String customerID = dataMap.get("CUSTOMERID").toString();
                                System.out.println("[MBLCKYC-UPLOAD]--CIFID: " + customerID);
                                String directoryPath = "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + customerID;
                                System.out.println("[MBLCKYC-UPLOAD]--LOCAL PATH:DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + customerID);
                                File directory = new File(directoryPath);
                                try {
                                    MBL_PINSTID = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
                                    System.out.println("[MBLCKYC-UPLOAD]-- SELECTING ID PROOF TYPE FOR PINSTID: " + MBL_PINSTID);

                                    //String idq = "SELECT PRIMARY_ID_TYPE AS ID_PROOF_TYPE FROM MBL_EXT WHERE PINSTID = ?";
                                    int pinstid_present = Integer.valueOf(dataMap.get("P_COUNT").toString());
                                    String idq;
                                    switch (pinstid_present) {
                                        case 1:
                                            idq = "SELECT PRIMARY_ID_TYPE AS ID_PROOF_TYPE FROM MBL_EXT WHERE PINSTID = ?";
                                            break;
                                        case 0:
                                            idq = "SELECT PRIMARY_ID_TYPE AS ID_PROOF_TYPE FROM MBL_EXTHISTORY WHERE PINSTID = ?";
                                            break;
                                        default:
                                            idq = "SELECT PRIMARY_ID_TYPE AS ID_PROOF_TYPE FROM MBL_EXT WHERE PINSTID = ?";
                                            break;

                                    }

                                    try (PreparedStatement pid = con.prepareStatement(idq)) {
                                        pid.setString(1, MBL_PINSTID);
                                        try (ResultSet idrs = pid.executeQuery()) {
                                            while (idrs.next()) {
                                                String ID_PROOF_NUMBER = String.valueOf(idrs.getString("ID_PROOF_TYPE"));
                                                System.out.println("[MBLCKYC-UPLOAD]--ID_PROOF_NUMBER: " + ID_PROOF_NUMBER);
                                                dataMap.put("ID_PROOF_NUMBER", ID_PROOF_NUMBER);
                                            }
                                        }
                                    }
                                } catch (SQLException e) {
                                    System.out.println("[MBLCKYC-UPLOAD]--ERROR OCCURED WHILE TRYING TO FETCH THE PRIMARY-ID TYPE");
                                    updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", e.getMessage());
                                    e.printStackTrace();
                                }
                                String idprooftype = dataMap.get("ID_PROOF_NUMBER").toString();
                                System.out.println("[MBLCKYC-UPLOAD]--PRIMARYIDPROOF_TYPE: " + idprooftype);
                                String[] documentNames;
                                switch (idprooftype) {
                                    case "1":
                                        System.out.println("[MBLCKYC-UPLOAD]--PRIMARY ID IS AADHAR");
                                        documentNames = new String[]{"Photograph", "Signature", "UID"};
                                        break;
                                    case "2":
                                        System.out.println("[MBLCKYC-UPLOAD]--PRIMARY ID IS VOTER");
                                        documentNames = new String[]{"Photograph", "Signature", "Voters_Identity_Card"};
                                        break;
                                    case "3":
                                        System.out.println("[MBLCKYC-UPLOAD]--PRIMARY ID IS Pancard");
                                        documentNames = new String[]{"Photograph", "Signature", "Pancard"};
                                        break;
                                    case "4":
                                        System.out.println("[MBLCKYC-UPLOAD]--PRIMARY ID IS Driving Licence");
                                        documentNames = new String[]{"Photograph", "Signature", "Driving_License"};
                                        break;
                                    case "6":
                                        System.out.println("[MBLCKYC-UPLOAD]--PRIMARY ID IS Passport");
                                        documentNames = new String[]{"Photograph", "Signature", "Passport"};
                                        break;
                                    default:
                                        documentNames = new String[]{"Photograph", "Signature", "UID"};
                                        System.out.println("[MBLCKYC-UPLOAD]--ID TYPE IS NEITHER 1 NOR 2NOW WE CONSIDER PRIMARY ID IS AADHAR");
                                        break;
                                }
                                if (directory.exists() && directory.isDirectory()) {
                                    File file = new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
                                    String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
                                    try (CSVWriter uniquewriter = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER)) {
                                        System.out.println("[MBLCKYC-UPLOAD]-- Writing all unique entries to the CSV file:");
                                        uniqueEntries.forEach((uniqueEntry) -> {
                                            System.out.println("[MBLCKYC-UPLOAD]---ENTRY INSIDE THE SET: " + uniqueEntry);
                                            String[] values = (uniqueEntry).split(",");
                                            uniquewriter.writeNext(values);
                                        });
                                    }

                                    File[] files = directory.listFiles();
                                    if (files != null) {
                                        for (String documentName : documentNames) {
                                            boolean found = false;
                                            for (File foundFile : files) {
                                                String fileNameWithoutExtension = FilenameUtils.removeExtension(foundFile.getName());
                                                foundDocument.add(foundFile.getName());
                                                if (fileNameWithoutExtension.equalsIgnoreCase(documentName)) {
                                                    found = true;
                                                    break;
                                                }
                                            }
                                            if (!found) {
                                                missingDocuments.add(documentName);
                                            }
                                        }
                                    }
                                }
                                missingDocumentsString = String.join(", ", (Iterable) missingDocuments);
                                foundDocument.forEach(fdocs -> System.out.println("Documents inside the folder: " + fdocs));
                                sftpdocs = String.join(", ", foundDocument);
                                System.out.println("[MBLCKYC-UPLOAD]--String of found documents: " + sftpdocs);
                                if (!missingDocuments.isEmpty()) {
                                    System.out.println("The following documents are missing:");
                                    missingDocuments.forEach(missingDocument -> System.out.println(missingDocument));
                                    try {
                                        System.out.println("[MBLCKYC-UPLOAD]--MANDATORY DOCUMENTS MISSING");
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
                                                if (file.isFile() && file.length() == 0L) {
                                                    System.out.println("Zero kb file found: " + file.getName());
                                                    size_zero.add(file.getName());
                                                }
                                            }
                                        } else {
                                            System.out.println("[MBLCKYC-UPLOAD]--Directory is empty.");
                                        }
                                    } else {
                                        System.out.println("[MBLCKYC-UPLOAD]--Directory does not exist or is not a directory.");
                                    }
                                    sftpstatus = (size_zero.size() > 0) ? 2 : 1;
                                    System.err.println("sftpstatus: " + sftpstatus);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            System.err.println("[MBLCKYC-UPLOAD]--sftpstatus for SFTP initiation: " + sftpstatus);
                            try {
                                MBL_PINSTID = String.valueOf(dataMap.get("PROCESSINSTANCEID"));
                                MBL_CIF = String.valueOf(dataMap.get("CIFID"));

                                switch (sftpstatus) {
                                    case 1: {

                                        String extquery = "UPDATE MBL_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "PASS");
                                            extstmt.setString(2, "FILES MOVED TO SFTP");
                                            extstmt.setString(3, MBL_PINSTID);
                                            extstmt.executeUpdate();
                                        }

                                        String uquery = "UPDATE MBL_RU_DMSARCHIVAL SET EXECUTION_FLAG = ?,REMARKS = ?,DOCS_IN_SFTP = ?,PROCESSED_AT=SYSDATE WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "UD");
                                            upstmt.setString(2, "FILES MOVED TO SFTP");
                                            upstmt.setString(3, sftpdocs);
                                            upstmt.setString(4, MBL_PINSTID);
                                            upstmt.executeUpdate();
                                        }

                                        System.out.println("[MBLCKYC-UPLOAD]--- Proceding For Sftp.");
                                        initiateSftp(configObj, con, dataMap);
                                        break;
                                    }
                                    case 2: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);

                                        String uquery = "UPDATE MBL_RU_DMSARCHIVAL SET EXECUTION_FLAG = ?,REMARKS = ?,DOCS_IN_SFTP = ?,PROCESSED_AT=SYSDATE WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "0kb File found");
                                            upstmt.setString(3, sftpdocs);
                                            upstmt.setString(4, MBL_PINSTID);
                                            upstmt.executeUpdate();
                                        }

                                        String extquery = "UPDATE MBL_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, "0 Kb file detected");
                                            extstmt.setString(3, MBL_PINSTID);
                                            extstmt.executeUpdate();
                                        }
                                        break;
                                    }
                                    case 3: {
                                        System.out.println("[DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);

                                        String uquery = "UPDATE MBL_RU_DMSARCHIVAL SET EXECUTION_FLAG = ?,REMARKS = ? ,MISSING_DOCS=?, DOCS_IN_SFTP = ?,PROCESSED_AT=SYSDATE WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "MISS");
                                            upstmt.setString(2, "FILES NOT FOUND");
                                            upstmt.setString(3, missingDocumentsString + " Not FOUND");
                                            upstmt.setString(4, sftpdocs);
                                            upstmt.setString(5, MBL_PINSTID);
                                            upstmt.executeUpdate();
                                        }

                                        String extquery = "UPDATE MBL_EXT SET Sftp_Status = ?,Sftp_Remarks = ? WHERE PINSTID = ?";
                                        try (PreparedStatement extstmt = con.prepareStatement(extquery)) {
                                            extstmt.setString(1, "FAIL");
                                            extstmt.setString(2, missingDocumentsString + " Not FOUND");
                                            extstmt.setString(3, MBL_PINSTID);
                                            extstmt.executeUpdate();
                                        }

                                        break;
                                    }
                                    default: {
                                        System.out.println("[FIG DOC_EXE Doc Upload] -- SFTP COULDN'T BE INITIATED");
                                        System.err.println("sftpstatus: " + sftpstatus);
                                        String uquery = "UPDATE MBL_RU_DMSARCHIVAL SET EXECUTION_FLAG = ?,REMARKS = ?,DOCS_IN_SFTP = ?,PROCESSED_AT=SYSDATE WHERE PROCESSINSTANCEID = ?";
                                        try (PreparedStatement upstmt = con.prepareStatement(uquery)) {
                                            upstmt.setString(1, "FAIL");
                                            upstmt.setString(2, "File Upload issue");
                                            upstmt.setString(3, sftpdocs);
                                            upstmt.setString(4, MBL_PINSTID);
                                            upstmt.executeUpdate();
                                        }
                                        break;
                                    }
                                }
                                sftpdocs = null;
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                            continue;
                        } else if (this.errorFlag.equalsIgnoreCase("DONE")) {
                            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "SKIP", "");
                        } else {
                            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", "ERROR WHILE FETHCING DOCS FROM DMS");
                        }
                    }
                    if (dataMap.get("TEMPFLAG").toString().equals("DONE") && dataMap.get("UPLOADFLAG").toString().equals("")) {
                        initiateSftp(this.configObj, con, dataMap);
                    }
                }
            }
            closeCallableConnection(rs, callableStatement);
        } catch (SQLException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            System.out.println("[MBLCKYC-UPLOAD]-- UPDATED MBL_RU_DMSARCHIVAL: EXCEPTION OCCURED FOR:" + dataMap.get("PROCESSINSTANCEID").toString());
            ex.getCause();
            System.out.println("[MBLCKYC-UPLOAD]--Inside Exception - " + ex);
        } finally {
            closeCallableConnection(rs, callableStatement);
        }
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    public int initiateDocUpload(Map dataMap, Long folderId, Connection con, String pinstid) {
        Map<Object, Object> Documents = new HashMap<>();
        int status = 0;
        try {
            DocumentRemoteModule documentModule = (DocumentRemoteModule) EJbContext.getContext("DocumentModuleServiceImpl", "SRV-DMS-APP", this.configObj.getDmsIp(), this.configObj.getDmsPort());
            System.out.println("[MBLCKYC-UPLOAD]-- DMS Ejb Object Created.");
            System.out.println("[MBLCKYC-UPLOAD]-- DMS-IP=" + this.configObj.getDmsIp());
            System.out.println("[MBLCKYC-UPLOAD]-- DMS-Port=" + this.configObj.getDmsPort());
            NodeFolder nf = new NodeFolder();
            nf.setUuid(folderId);
            System.out.println("[MBLCKYC-UPLOAD]-- - sessionId-" + this.sessionId);
            this.tokenMap = checkDMSSession(con, status, this.configObj);
            if (this.tokenMap.get("status").equals("1")) {
                this.sessionId = this.tokenMap.get("sessionId").toString();
            }
            if (!this.sessionId.equals("")) {
                List<NodeDocument> nodeDoc = documentModule.getDocumentList(this.sessionId, (Repository) nf);
                System.out.println("[MBLCKYC-UPLOAD]-- Document Details Fetched.");
                if (!nodeDoc.isEmpty()) {
                    for (NodeDocument nodeD : nodeDoc) {
                        System.out.println(nodeD.getUuid());
                        System.out.println(nodeD.getName());
                        dataMap.put("DocName", nodeD.getName());
                        dataMap.put("EXT", nodeD.getExt());
                        Long versn = Long.valueOf(nodeD.getCurrentVersion().getVersion());
                        if (nodeD.getName().equalsIgnoreCase("Group Photo.") || nodeD.getName().equalsIgnoreCase("Group Photo")) {
                            System.out.println("File Skipped");
                            continue;
                        }
                        try (ByteArrayInputStream is = new ByteArrayInputStream(documentModule.getDocumentContent(this.sessionId, nodeD.getUuid(), versn))) {
                            System.out.println("[MBLCKYC-UPLOAD]-- - Going to extract Docs.");
                            String docExtName = dataMap.get("DocName").toString();
                            System.out.println("[MBLCKYC-UPLOAD]-- - docExtName---" + docExtName);
                            String DocTypeName = docExtName;
                            dataMap.put("ProofName", docExtName);
                            dataMap.put("CsvName", DocTypeName);
                            if (docExtName.equalsIgnoreCase("Group Photo.")) {
                                docExtName = "Group Photo";
                                dataMap.put("ProofName", docExtName);
                                dataMap.put("CsvName", docExtName);
                            }
                            if (docExtName.equalsIgnoreCase("Customer Photo")) {
                                docExtName = "Photograph";
                                dataMap.put("ProofName", docExtName);
                                dataMap.put("CsvName", docExtName);
//                            dataMap.put("CsvName", "Customer Photo");
                            }
                            status = fileOperation(is, dataMap);
                        }
                    }
                    System.out.println("[MBLCKYC-UPLOAD]-- For Loop Stopped");
                    System.out.println("[MBLCKYC-UPLOAD]-- All Doc Extracted Initiating DocMerge");
                    try {
                        cKycupdate(dataMap, con, pinstid);
                    } catch (Exception ex) {
                        updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
                        System.out.println("[MBLCKYC-UPLOAD]-- UPDATED MBL_RU_DMSARCHEIVAL: EXCEPTION OCCURED FOR:" + dataMap.get("PROCESSINSTANCEID").toString());
                        ex.getCause();
                        System.out.println("[MBLCKYC-UPLOAD]-- Inside Exception - " + ex);
                        ex.printStackTrace();
                    }
                } else {
                    status = 0;
                    System.out.println("[MBLCKYC-UPLOAD]-- The List nodeDoc is empty");
                    System.out.println("[MBLCKYC-UPLOAD]-- Sending status= 0");
                }
            } else {
                System.out.println("[MBLCKYC-UPLOAD]-- Session Not Found To Upload Docs");
            }
        } catch (InvalidRepositoryException | com.servo.dms.exception.PathNotFoundException | IOException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            System.out.println("[MBLCKYC-UPLOAD]-- UPDATED MBL_RU_DMSARCHIVAL: EXCEPTION OCCURED FOR:" + dataMap.get("PROCESSINSTANCEID").toString());
            ex.getCause();
            System.out.println("[MBLCKYC-UPLOAD]-- EX -" + ex);
            if (ex.getMessage().contains("com.servo.dms.exception.RepositoryException: File Not Found for")) {
                this.errorFlag = "DONE";
            }
            return status;
        }
        return status;
    }

    public int fileOperation(ByteArrayInputStream is, Map<String, String> dataMap) {
        CSVWriter writer = null;
        int status = 0;
        try {
            String docName = "";
            String extension = "";
            File file = new File("DMS_ARCHIVAL");
            if (!file.exists()) {
                boolean b = file.mkdirs();
                System.out.println("[MBLCKYC-UPLOAD]-- - Base Directory Created.");
            }
            System.out.println("---Absolute path-1-" + file.getAbsolutePath());
            file = new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            this.configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            dataMap.put("TEMPLOC", this.configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            System.out.println("---Absolute path-2---" + file.getAbsolutePath());
            if (!file.exists()) {
                file.mkdir();
                System.out.println("[MBLCKYC-UPLOAD]-- - Child Folder Created.");
                writer = new CSVWriter(new FileWriter(csv), '|', CSVWriter.NO_QUOTE_CHARACTER);
                String[] header = "FILENAME,DC.WINAME,DC.ACCNO,DC.CIFNO,DOCTYPE".split(",");
                writer.writeNext(header);
                writer.close();
            }
            docName = dataMap.get("DocName").toString();
            extension = dataMap.get("EXT").toString();
            System.out.println("[MBLCKYC-UPLOAD]---- Extension found- " + extension);
            System.out.println("[MBLCKYC-UPLOAD]---- DocumentName- " + docName);
            String docExtName = dataMap.get("ProofName");
            String DocTypeName = dataMap.get("CsvName");
            System.out.println("[MBLCKYC-UPLOAD]---- File opertaion docExtName- " + docExtName);
            System.out.println("[MBLCKYC-UPLOAD]---- File opertaion docExtName- " + DocTypeName);

            if (docExtName.equalsIgnoreCase("MBL CUSTOMER PHOTO")) {
                docExtName = "Photograph";
            } else if (docExtName.equalsIgnoreCase("MBL PAN CARD")) {
                docExtName = "Pancard";
            } else if (docExtName.equalsIgnoreCase("MBL SIGNATURE OR THUMB")) {
                docExtName = "Signature";
            } else if (docExtName.equalsIgnoreCase("MBL FORM60")) {
                docExtName = "Form60";
            }

            if (extension.equalsIgnoreCase("pdf")) {
                byte[] array = new byte[is.available()];
                is.read(array);
                try (FileOutputStream fos = new FileOutputStream("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension)) {
                    fos.write(array);
                }
                System.out.println("[MBLCKYC-UPLOAD]-- UPLOAD FILE - DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension);
            } else {
                BufferedImage bImageFromConvert = ImageIO.read(is);
                ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension));
            }

            System.out.println("[MBLCKYC-UPLOAD]-- Doc Copied To Server Locations--" + DocTypeName);
//            if (!docExtName.trim().startsWith("MBL Corrs Id") && !docExtName.trim().startsWith("MBL Primary Id") && !docExtName.trim().startsWith("MBL Bank Passbook")) {
//                System.out.println("[MBLCKYC-UPLOAD]-- Inside the if loop" + docExtName);
//                writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
//                String[] values = (docExtName + "." + extension + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + docExtName).split(",");
//                writer.writeNext(values);
//                writer.close();
//            }

            if (!docExtName.trim().startsWith("MBL Corrs Id") && !docExtName.trim().startsWith("MBL Primary Id") && !docExtName.trim().startsWith("MBL Bank Passbook")) {
                System.out.println("[MBLCKYC-UPLOAD]-- Inside the if loop" + docExtName);
                String entry = docExtName + "." + extension + ","
                        + dataMap.get("PROCESSINSTANCEID") + ","
                        + dataMap.get("ACCOUNTNO") + ","
                        + dataMap.get("CIFID") + ","
                        + DocTypeName;

                if (uniqueEntries.add(entry)) {
                    System.out.println("[MFCKYC-UPLOAD]-- Added the data to set for csv writing.");
                } else {
                    System.out.println("[MFCKYC-UPLOAD]-- Duplicate entry skipped for " + DocTypeName);
                }
            }

            System.out.println("[MBLCKYC-UPLOAD]-- Csv Entry Created");
            status = 1;
        } catch (Exception ex) {
            System.out.println("[MBLCKYC-UPLOAD]-- - File writing exe - " + ex);
            ex.printStackTrace();
            return status;
        }
        return status;
    }

    public void cKycupdate(Map<String, String> dataMap, Connection con, String pinstid) {
        //CSVWriter writer = null;
        int status = 0;
        String fileA = "";
        File testFile = null;
        String fileB = "";
        String path = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
        try {
            Map HashCustProof = getDocuemntsDetails(con, pinstid, dataMap);
            System.out.println("[MBLCKYC-UPLOAD]-- HashCustProof---" + HashCustProof.toString());

            String IdProoftype = HashCustProof.get("IDPROOFTYPE").toString();
            System.out.println("[[MBLCKYC-UPLOAD]-- IdProoftype---" + IdProoftype);

            String AddressProofType = HashCustProof.get("ADDRESS_PROOF_TYPE").toString();
            System.out.println("[MBLCKYC-UPLOAD]-- AddressProoftype---" + AddressProofType);
            System.out.println("[MBLCKYC-UPLOAD]-- Completed the CKYC Documents Chages");

            File file = new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[MBLCKYC-UPLOAD]-- CKyc-- File-" + file.getAbsolutePath());

            this.configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            System.out.println("[MBLCKYC-UPLOAD]-- TEMPLOC-- SFTP-" + this.configObj.getDmsSftpLocalDir());
            dataMap.put("TEMPLOC", this.configObj.getDmsSftpLocalDir());
            String csv = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + ".csv";
            //writer = new CSVWriter(new FileWriter(csv, true), '|', CSVWriter.NO_QUOTE_CHARACTER);
            System.out.println("[MBLCKYC-UPLOAD]-- CKyc-- csv file-" + csv);
            List<String> files = new ArrayList<>();

            String StrVal = "MBL Primary Id,MBL Corrs Id,MBL Bank Passbook";

            String[] arrSplit = StrVal.split(",");
            String StatusValue = "S";
            String csvName = "";
            String proofName = "";
            for (String strTemp : arrSplit) {
                System.out.println("[MBLCKYC-UPLOAD]--1--Working==" + strTemp);
                System.out.println("[MBLCKYC-UPLOAD]--Status---" + StatusValue);
                if (IdProoftype.equalsIgnoreCase("1") && AddressProofType.equalsIgnoreCase("1") && StatusValue.equalsIgnoreCase("S") && !dataMap.get("EKYCFLAG").toString().equalsIgnoreCase("Y")) {
                    fileA = path + File.separator + "MBL Primary Id Front.jpg";
                    fileB = path + File.separator + "MBL Primary Id BACK.jpg";
                    csvName = "Address_ID Proof";
                    proofName = "UID.pdf";
                    StatusValue = "Y";
                    System.out.println("2--Working==");
                } else if (strTemp.equalsIgnoreCase("MBL ADDRESS PROOF")) {
                    System.out.println("3--Working==");
                    fileA = path + File.separator + "MBL ADDRESS PROOF FRONT.jpg";
                    fileB = path + File.separator + "MBL ADDRESS PROOF BACK.jpg";
                    csvName = strTemp;
                    System.out.println("[MBLCKYC-UPLOAD]--Status---" + StatusValue);
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
                    System.out.println("[MBLCKYC-UPLOAD]--4--Working==");
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
                    System.out.println("[MBLCKYC-UPLOAD]--5--Working==");

                    fileA = path + File.separator + strTemp + " Front.jpg";
                    fileB = path + File.separator + strTemp + " Back.jpg";

                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    proofName = strTemp + ".pdf";
                    StatusValue = "Y";
                } else if (strTemp.equalsIgnoreCase("MBL Bank Passbook")) {
                    System.out.println("[MBLCKYC-UPLOAD]--5--Working==");

                    fileA = path + File.separator + strTemp + " Image page1.jpg";
                    fileB = path + File.separator + strTemp + " Image page2.jpg";

                    csvName = strTemp;
                    System.out.println("Status---" + StatusValue);
                    //proofName = strTemp + ".pdf";
                    proofName = "Bank_Statement.pdf";
                    StatusValue = "Y";
                }
                File file_front = new File(fileA);

                File file_back = new File(fileB);

                String entry = null;

                if (file_front.exists() && file_back.exists()) {
                    files.add(fileA);
                    files.add(fileB);
                    System.out.println("[MBLCKYC-UPLOAD]---BOTH FRONT AND BACK ID PROOFS ARE VAILABLE: ");
                    System.out.println("[MBLCKYC-UPLOAD]---fileA--" + fileA);
                    System.out.println("[MBLCKYC-UPLOAD]---fileB--" + fileB);
                    System.out.println("[MBLCKYC-UPLOAD]--Pdf upload csvname: " + csvName);
                    ImageToPDF(files, path + File.separator + proofName);
                    entry = proofName + ","
                            + dataMap.get("PROCESSINSTANCEID") + ","
                            + dataMap.get("ACCOUNTNO") + ","
                            + dataMap.get("CIFID") + ","
                            + csvName;

                    // Check if the entry is unique
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
                } else if (!file_front.exists() && !file_back.exists()) {
                    // Neither file exists
                    System.out.println("[MBLCKYC-UPLOAD]---NEITHER FRONT NOR BACK ID PROOFS ARE AVAILABLE.");
                    // Handle the case where neither file exists, if needed
                } else if (file_front.exists() && !file_back.exists()) {
                    System.out.println("[MBLCKYC-UPLOAD]---BACK ID PROOF IS NOT AVAILABLE.");
                    entry = strTemp + " Front.jpg" + ","
                            + dataMap.get("PROCESSINSTANCEID") + ","
                            + dataMap.get("ACCOUNTNO") + ","
                            + dataMap.get("CIFID") + ","
                            + csvName;
                } else if (!file_front.exists() && file_back.exists()) {
                    System.out.println("[MBLCKYC-UPLOAD]---FRONT ID PROOF IS NOT AVAILABLE.");
                    entry = strTemp + " Back.jpg" + ","
                            + dataMap.get("PROCESSINSTANCEID") + ","
                            + dataMap.get("ACCOUNTNO") + ","
                            + dataMap.get("CIFID") + ","
                            + csvName;
                }
                if (entry != null) {
                    if (uniqueEntries.add(entry)) {
                        System.out.println("[MBLCKYC-UPLOAD]-- Added the data to set for csv writing.");
                    } else {
                        System.out.println("[MBLCKYC-UPLOAD]-- Duplicate entry skipped for " + proofName);
                    }

                }
//                testFile = new File(fileA);
//                if (testFile.exists()) {
//                    files.add(fileA);
//                    files.add(fileB);
//                    System.out.println("[MBLCKYC-UPLOAD]---fileA--" + fileA);
//                    System.out.println("[MBLCKYC-UPLOAD]---fileA--" + fileB);
//                    System.out.println("[MBLCKYC-UPLOAD]--Pdf upload csvname: " + csvName);
//                    ImageToPDF(files, path + File.separator + proofName);
//                    String[] values = (proofName + "," + dataMap.get("PROCESSINSTANCEID") + "," + dataMap.get("ACCOUNTNO") + "," + dataMap.get("CIFID") + "," + csvName).split(",");
//                    writer.writeNext(values);
//                    if (StatusValue.equalsIgnoreCase("Y")) {
//                        DeleteFile(files);
//                        files.clear();
//                        fileA = path + File.separator + "ID Proof Front.jpg";
//                        fileB = path + File.separator + "ID Proof Back.jpg";
//                        files.add(fileA);
//                        files.add(fileB);
//                        DeleteFile(files);
//                    } else {
//                        DeleteFile(files);
//                    }
//                    files.clear();
//                } else {
//                    System.out.println("[MBLCKYC-UPLOAD]-- Doc Not Found-Address Proof");
//                }
            }
            //writer.close();
        } catch (Exception fx) {
            System.out.println("[MBLCKYC-UPLOAD]-- Updating File not found: " + fx.getMessage());
            updateFlag(con, dataMap.get("PROCESSINSTANCEID"), "ERR", "File is not created");
            System.out.println("[MBLCKYC-UPLOAD]-- Updating File ");
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
        } catch (Exception e) {
            System.out.println("[MBLCKYC-UPLOAD]-- - C Kyc Doc Write Exc- " + e);
        }
    }

    public void initiateSftp(propertyConfig configObj, Connection con, Map dataMap) {
        StandardFileSystemManager manager = new StandardFileSystemManager();
        String serverAddress = configObj.getDmsSftpIp();
        String userId = configObj.getDmsSftpUserId();
        String password = configObj.getDmsSftpPswd();
        String remoteDirectory;
        //remoteDirectory = configObj.getDmsSftpDir();

        //PROD
        remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/IN24/";
        //UAT
        //String remoteDirectory = "BPMDOCUPLOAD/IN/";

        System.out.println("[FIGCKYC]--REMOTE DIR: " + remoteDirectory);
        String localDirectory = "DMS_ARCHIVAL/MBL/SB_CKYC/";
        System.out.println("[FIGCKYC]--LOCAL DIR: " + localDirectory);
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
            System.out.println("[MBLCKYC-UPLOAD]-- - Local Dir -" + localDirectory + fileToFTP);
            System.out.println("[MBLCKYC-UPLOAD]-- - check if the file exists");
            String filepath = localDirectory + fileToFTP;
            File file = new File(filepath);
            if (!file.exists()) {
                throw new RuntimeException("[MBLCKYC-UPLOAD]-- Error. Local file not found");
            }
            System.out.println("[MBLCKYC-UPLOAD]-- - Initializes the file manager");
            manager.init();
            System.out.println("[MBLCKYC-UPLOAD]-- - Setup our SFTP configuration");
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, Integer.valueOf(10000));
            password = URLEncoder.encode(password);
            System.out.println("[MBLCKYC-UPLOAD]-- - Create the SFTP URI using the host name, userid, password,  remote path and file name");
            String sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/" + remoteDirectory + fileToFTP;
            System.out.println("[MBLCKYC-UPLOAD]-- --" + sftpUri);
            System.out.println("[MBLCKYC-UPLOAD]-- - Create local file object");
            FileObject localFile = manager.resolveFile(file.getAbsolutePath());
            System.out.println("[MBLCKYC-UPLOAD]-- - Create remote file object");
            FileObject remoteFile = manager.resolveFile(sftpUri, opts);
            System.out.println("[MBLCKYC-UPLOAD]-- -- Copy local file to sftp server");
            remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
            System.out.println("[MBLCKYC-UPLOAD]-- -- Folder upload successful");
            File localDir = new File(localDirectory + fileToFTP);
            File[] subFiles = localDir.listFiles();
            for (File item : subFiles) {
                sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/" + remoteDirectory + fileToFTP + File.separator + item.getName();
                file = new File(localDirectory + fileToFTP + File.separator + item.getName());
                System.out.println("[MBLCKYC-UPLOAD]-- -- " + file.getAbsolutePath());
                localFile = manager.resolveFile(file.getAbsolutePath());
                remoteFile = manager.resolveFile(sftpUri, opts);
                if (item.isFile()) {
                    remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                    cSftp.chmod(511, remoteDirectory + fileToFTP + "/" + item.getName());
                    System.out.println("[MBLCKYC-UPLOAD]-- -- File upload successful");
                }
            }
            cSftp.chmod(511, remoteDirectory + "/" + dataMap.get("TEMPDOC_LOCATION").toString());
            System.out.println("[MBLCKYC-UPLOAD]-- -- Updating Flag.");
            if (updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "")) {
                System.out.println("[MBLCKYC-UPLOAD]-- Routing Done for - " + dataMap.get("PROCESSINSTANCEID").toString());
                File fileBkp = new File("DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC" + File.separator + "EXT-" + dataMap.get("CUSTOMERID"));
                String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "MBL" + File.separator + "SB_CKYC";
                String folderToDelete = "EXT-" + deletecustomerID;
                File directoryToDelete = new File(deletedirectoryPath, folderToDelete);

                String deletestatus = "SELECT CKYC_DOC_DELETE FROM MF_MS_CKYC_DEL_STATUS WHERE PROCESS= ? ";
                String d_status = "N";
                try {
                    try (PreparedStatement dpt = con.prepareStatement(deletestatus)) {
                        dpt.setString(1, "MBL");
                        try (ResultSet drs = dpt.executeQuery()) {
                            while (drs.next()) {
                                d_status = drs.getString("CKYC_DOC_DELETE");
                            }
                        }

                    }
                } catch (SQLException e) {
                    System.out.println("[MBLCKYC-UPLOAD]--EXCEPTION OCCURED: " + e.getMessage());
                    updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", e.getMessage());
                }

                switch (d_status) {
                    case "Y": {
                        if (directoryToDelete.exists()) {
                            System.out.println("[MBLCKYC-UPLOAD]--CKYC DOCUMENT DELETE STATUS IS SET AS YES --Directory Found -- Deleting Existing Folder After upload to sftp: EXT-" + deletecustomerID);
                            deleteDirectory(directoryToDelete);
                        } else {
                            System.out.println("[MBLCKYC-UPLOAD]--CKYC DOCUMENT DELETE STATUS IS SET AS YES --Directory does not exist.");
                        }
                        break;
                    }
                    case "N": {
                        System.out.println("[MBLCKYC-UPLOAD]--CKYC DOCUMENT DELETE STATUS IS SET AS NO -- KEEPING THE CKYC DOCUMENTS.");
                        break;
                    }
                    default: {
                        if (directoryToDelete.exists()) {
                            System.out.println("[MBLCKYC-UPLOAD]-- CKYC DOCUMENT DELETE STATUS IS SET AS YES --Directory Found -- Deleting Existing Folder After upload to sftp: EXT-" + deletecustomerID);
                            deleteDirectory(directoryToDelete);
                        } else {
                            System.out.println("[MBLCKYC-UPLOAD]-- CKYC DOCUMENT DELETE STATUS IS SET AS YES --Directory does not exist.");
                        }
                        break;
                    }
                }

//                if (directoryToDelete.exists()) {
//                    System.out.println("[MBLCKYC-UPLOAD]--Directory Found -- Deleting Existing Folder After upload to sftp: EXT-" + deletecustomerID);
//                    deleteDirectory(directoryToDelete);
//                } else {
//                    System.out.println("[MBLCKYC-UPLOAD]--Directory does not exist.");
//                }
            }
        } catch (JSchException | com.jcraft.jsch.SftpException | RuntimeException | org.apache.commons.vfs.FileSystemException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "EXC", ex.getMessage());
            System.out.println("[MBLCKYC-UPLOAD]-- UPDATED FIG_RU_DMSARCHEIVAL: EXCEPTION OCCURED FOR:" + dataMap.get("PROCESSINSTANCEID").toString());
            System.out.println("[MBLCKYC-UPLOAD]-- --File upload Ex - " + ex);
        } finally {
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
                query = "UPDATE MBL_RU_DMSARCHIVAL SET EXECUTION_FLAG = ? ,PROCESSED_AT = SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "UD");
                pstmt.setString(2, pinstId);
            } else if (type.equalsIgnoreCase("ERR")) {
                query = "UPDATE MBL_RU_DMSARCHIVAL SET EXECUTION_FLAG = ?,REMARKS = ? ,PROCESSED_AT = SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "E");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);
            } else if (type.equalsIgnoreCase("EXC")) {
                query = "UPDATE MBL_RU_DMSARCHIVAL SET EXECUTION_FLAG = ? ,REMARKS=? ,PROCESSED_AT=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "EXC");
                pstmt.setString(2, tempLocation);
                pstmt.setString(3, pinstId);
            }
            pstmt.executeUpdate();
            closeConnection(null, pstmt);
        } catch (Exception ex) {
            System.out.println("[MBLCKYC-UPLOAD]-- -- UPdate Status Eex - " + ex);
            return Boolean.FALSE.booleanValue();
        } finally {
            closeConnection(null, pstmt);
        }
        return Boolean.TRUE.booleanValue();
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

    public Map getDocuemntsDetails(Connection con, String strPinstid, Map dataMap) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String branchName = "";
        Map<Object, Object> dataProof = new HashMap<>();
        try {

            int pinstid_present = 0;

            String curretn_case = "SELECT COUNT(PINSTID) AS P_COUNT FROM MBL_EXT WHERE PINSTID = ?";
            try (PreparedStatement cpr = con.prepareStatement(curretn_case)) {
                cpr.setString(1, strPinstid);
                try (ResultSet idrs = cpr.executeQuery()) {
                    while (idrs.next()) {
                        pinstid_present = idrs.getInt("P_COUNT");
                        dataMap.put("P_COUNT", pinstid_present);
                        System.out.println("[MBLCKYC-UPLOAD]--Work-item live status TO FETCH THE IDPROOF TYPE: " + pinstid_present);

                    }
                }
            }

            String ddq;
            switch (pinstid_present) {
                case 1:
                    ddq = "SELECT PRIMARY_ID_TYPE,ADDRESS_PROOF_TYPE FROM MBL_EXT WHERE PINSTID =?";
                    break;
                case 0:
                    ddq = "SELECT PRIMARY_ID_TYPE,ADDRESS_PROOF_TYPE FROM MBL_EXTHISTORY WHERE PINSTID =?";
                    break;
                default:
                    ddq = "SELECT PRIMARY_ID_TYPE,ADDRESS_PROOF_TYPE FROM MBL_EXT WHERE PINSTID =?";
                    break;

            }

            System.out.println("[MF-CKYC]--QUERY TO FETCH THE IDPROOF TYPE: " + ddq);

            try (PreparedStatement pid = con.prepareStatement(ddq)) {
                pid.setString(1, strPinstid);
                try (ResultSet idrs = pid.executeQuery()) {
                    while (idrs.next()) {
                        dataProof.put("IDPROOFTYPE", idrs.getString("PRIMARY_ID_TYPE"));
                        dataProof.put("ADDRESS_PROOF_TYPE", idrs.getString("ADDRESS_PROOF_TYPE"));
                    }
                }
            }

//            query = "SELECT PRIMARY_ID_TYPE,ADDRESS_PROOF_TYPE FROM MBL_EXT WHERE PINSTID =?";
//            pstmt = con.prepareStatement(query);
//            pstmt.setString(1, strPinstid);
//            rs = pstmt.executeQuery();
//            while (rs.next()) {
//                dataProof.put("IDPROOFTYPE", rs.getString("PRIMARY_ID_TYPE"));
//                dataProof.put("ADDRESS_PROOF_TYPE", rs.getString("ADDRESS_PROOF_TYPE"));
//            }
            closeConnection(rs, pstmt);
        } catch (SQLException ex) {
            updateFlag(con, strPinstid, "EXC", ex.getMessage());
            return dataProof;
        } finally {
            closeConnection(rs, pstmt);
        }
        return dataProof;
    }

    public HashMap checkDMSSession(Connection con, int userId, propertyConfig config) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String query = "";
        int count = 0;
        SessionService service = new SessionService();
        HashMap<Object, Object> sessionMap = new HashMap<>();
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
        } catch (SQLException ex) {
            ex.printStackTrace();
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

    public void DeleteFile(List<String> files) {
        for (int i = 0; i < files.size(); i++) {
            File ob = new File(files.get(i));
            ob.delete();
        }
    }
}
