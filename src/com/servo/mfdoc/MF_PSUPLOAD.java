/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.mfdoc;

/**
 *
 * @author VINITH
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import au.com.bytecode.opencsv.CSVWriter;
import com.servo.beandoc.RespBean;
import com.servo.businessdoc.SessionService;
import com.servo.configdoc.EJbContext;
import com.servo.configdoc.LoadConfig;
import com.servo.config.SRVTimerServiceConfig;
import com.servo.configdoc.propertyConfig;
import com.servo.dms.entity.NodeDocument;

import com.servo.dms.entity.NodeFolder;
import com.servo.dms.exception.InvalidRepositoryException;
import com.servo.dms.exception.PathNotFoundException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Execute;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.TimerService;
import javax.ejb.EJBContext;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import com.servo.dms.module.remote.DocumentRemoteModule;
import com.servo.servicedoc.ImageCompress;
import com.servo.servicedoc.ProfileIntg;
import com.servo.util.SRVUtil;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author Saravanan
 */
@TimerService

public class MF_PSUPLOAD {

    @Resource
    private SRVTimerServiceConfig Timer_Service_Id;

    @Resource
    EJBContext etx;

    propertyConfig configObj = new propertyConfig();

    SessionService service = new SessionService();

    HashMap tokenMap = new HashMap<>();

    String sessionId = "";

    String errorFlag = "";

    List<NodeDocument> nodeDoc;

    @PostConstruct
    public void loadConfigurations(Connection con1) throws NoSuchProviderException, MessagingException {
        try {
            System.out.println("[initPhotoSigUpload] - Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(this.configObj);
            System.out.println("--configIP-" + this.configObj.getIstreamsIp());
            System.out.println("--configPORT-" + this.configObj.getDmsPort());
            this.tokenMap = this.service.getSession(this.configObj.getIstreamsIp(), this.configObj.getIstreamsPort(), "MFDMSUSER1", "System1234#");
            System.out.println("tokenMap--" + this.tokenMap.get("status"));
            if (this.tokenMap.get("status").equals("1")) {
                this.sessionId = this.tokenMap.get("sessionId").toString();
                System.out.println("session id===" + this.sessionId);
            }
        } catch (Exception ex) {
            
            System.out.println("[initPhotoSigUpload] - Config Load Exception->" + ex.getMessage());
            ex.printStackTrace();
        }
    }

    @Execute
    public void execute(Connection con) throws Exception {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long folderId = 0L;
        String pinstid = null;
        Map dataMap = new HashMap<>();
        try {
            System.out.println("[initPhotoSigUpload] -- Going To Fetch Case For Doc Archival.");
            query = "SELECT  A.Folderid AS FOLDERID,A.Processinstanceid AS PROCESSINSTANCEID ,A.Accountno AS ACCOUNTNO,A.Cifid AS CIFID From Mf_Ru_Dmsarchival A   WHERE A.EXECUTION_FLAG = ? and rownum <= ? ";
            pstmt = con.prepareCall(query);
            pstmt.setString(1, "PS");
            pstmt.setString(2, "1");
            rs = pstmt.executeQuery();
            while (rs.next()) {
                folderId = rs.getLong("FOLDERID");
                pinstid = rs.getString("PROCESSINSTANCEID");
                dataMap.put("ACTIVITYID", "NA");
                dataMap.put("ACCOUNTNO", rs.getString("ACCOUNTNO"));
                dataMap.put("CIFID", rs.getString("CIFID"));
                dataMap.put("CUSTOMERID", rs.getString("CIFID"));
                dataMap.put("pinstid", pinstid);
                dataMap.put("PROCESSINSTANCEID", pinstid);
            }
            closeConnection(rs, pstmt);
            if (pinstid != null && folderId != 0L) {
                System.out.println("[initPhotoSigUpload] - Data Fetched Going to initiate Doc Upload: " + pinstid);
//                String parentName = "MFDMS_ARCHIVAL_PHOTO_SIG_NEW" + File.separator + "EXT-" + dataMap.get("CIFID");
                String parentName = "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG" + File.separator + "EXT-" + dataMap.get("CIFID");
                int Status = initiateDocUpload(dataMap, folderId, con);
                System.out.println("[MF-PS]-- DOCWRITE STATUS Status:" + Status);
                System.out.println("[MF-PS]-- Proceding to update the flag for : " + dataMap.get("PROCESSINSTANCEID"));
                Map<String, String> Resultmap;
                Resultmap = DocUpload(con, dataMap);
                System.out.println("[MF-PS]--Resultmap: " + Resultmap.toString());
                if (((String) Resultmap.get("SIGSTATUS")).equalsIgnoreCase("PASS") && ((String) Resultmap.get("PICSTATUS")).equalsIgnoreCase("PASS")) {
                    updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "");
                    String deletecustomerID = dataMap.get("CUSTOMERID").toString();
                    String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG";
                    String folderToDelete = "EXT-" + deletecustomerID;

                    File directoryToDelete = new File(deletedirectoryPath, folderToDelete);
                    if (directoryToDelete.exists()) {
                        System.out.println("Directory Found -- Deleting Existing Folder beacause PS_UPLOAD IS SUCCESS FOR: EXT-" + deletecustomerID);
                        deleteDirectory(directoryToDelete);
                    } else {
                        System.out.println("Directory does not exist.");
                    }
                } else if (((String) Resultmap.get("SIGSTATUS")).equalsIgnoreCase("FAIL") || ((String) Resultmap.get("PICSTATUS")).equalsIgnoreCase("FAIL")) {
                    updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "ERR", Resultmap.get("SIGMSG") + " | " + Resultmap.get("PICMSG"));
                } else if (!((String) Resultmap.get("SIGSTATUS")).equalsIgnoreCase("FAIL") && !((String) Resultmap.get("PICSTATUS")).equalsIgnoreCase("FAIL")) {
                    if (((String) Resultmap.get("SIGSTATUS")).equalsIgnoreCase("FAIL") || ((String) Resultmap.get("PICSTATUS")).equalsIgnoreCase("FAIL"));
                }
                //File fileBkp = new File(parentName);
                //removeDirectory(fileBkp);

            }
        } catch (SQLException ex) {
            System.out.println("[initArchivalRequest]-{execute}-EX- " + ex.getMessage());
            ex.printStackTrace();
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

    public static void removeDirectory(File dir) {
        System.out.println("Start Deleteing the Documents ");
        try {
            if (dir.isDirectory()) {
                System.out.println("File Directory:" + dir.toString());
                File[] files = dir.listFiles();
                if (files != null && files.length > 0) {
                    System.out.println("Folder have list of files");
                    for (File aFile : files) {
                        System.out.println("File for deleteing " + aFile.getAbsolutePath());
                        removeDirectory(aFile);
                    }
                }
                dir.delete();
            } else {
                dir.delete();
            }
        } catch (Exception e) {
            System.out.println("Exeption For deleting the documents ");
            System.out.println("Exception----" + e.getMessage());
        }
    }

    public int initiateDocUpload(Map<String, String> dataMap, Long folderId, Connection con) throws PathNotFoundException {
        int status = 0;
        try {
            DocumentRemoteModule documentModule = (DocumentRemoteModule) EJbContext.getContext("DocumentModuleServiceImpl", "SRV-DMS-APP", this.configObj.getDmsIp(), this.configObj.getDmsPort());
            System.out.println("[MF-PS]-- DMS Ejb Object Created.");
            String pinstId = dataMap.get("pinstid");
            System.out.println("[MF-PS]-- session id--" + this.sessionId);
            NodeFolder nf = new NodeFolder();
            nf.setUuid(folderId);
            this.tokenMap = checkDMSSession(con, 216133, this.configObj, this.sessionId);
            if (this.tokenMap.get("status").equals("1")) {
                this.sessionId = this.tokenMap.get("sessionId").toString();
            }
            if (!this.sessionId.equals("")) {
                this.nodeDoc = documentModule.getDocumentList(sessionId, nf);
                System.out.println("[MF-PS]-- Document Details Fetched.");
                if (!this.nodeDoc.isEmpty()) {
                    for (NodeDocument nodeD : this.nodeDoc) {
                        System.out.println(nodeD.getUuid());
                        System.out.println(nodeD.getName());
                        dataMap.put("DocName", nodeD.getName());
                        dataMap.put("EXT", nodeD.getExt());
                        Long versn = Long.valueOf(nodeD.getCurrentVersion().getVersion());
                        System.out.println("[MF-PS]-- Going to extract Docs.");
                        String docExtName = dataMap.get("DocName");
                        System.out.println("[MF-PS]-- - docExtName---" + docExtName);
                        String DocTypeName = docExtName;
                        dataMap.put("ProofName", docExtName);
                        dataMap.put("CsvName", DocTypeName);
                        String strCif = dataMap.get("CUSTOMERID");
                        
                        
//                        String csvName = null;
//                        String proofNameSuffix = null;
//
//                        if (docExtName.equalsIgnoreCase("Customer Photo")) {
//                            proofNameSuffix = "pic";
//                            csvName = "Customer Photo";
//                        } else if (docExtName.equalsIgnoreCase("Signature")) {
//                            proofNameSuffix = "sig";
//                            csvName = "Siganture";
//                        }
//
//                        if (proofNameSuffix != null) {
//                            docExtName = "c-" + strCif + "-" + proofNameSuffix;
//                            dataMap.put("ProofName", docExtName);
//                            dataMap.put("CsvName", csvName);
//
//                            try (ByteArrayInputStream is = new ByteArrayInputStream(documentModule.getDocumentContent(this.sessionId, nodeD.getUuid(), versn))) {
//                                status = MFfileOperation(is, dataMap, con);
//                                System.out.println("[MF-PS]--FILE WAS WRITTEN SUCCESSFULLY FOR: " + dataMap.get("ProofName") + " : "+ dataMap.get("PROCESSINSTANCEID"));
//                            } catch (PathNotFoundException ex) {
//                                Logger.getLogger(MF_PSUPLOAD.class.getName()).log(Level.SEVERE, null, ex);
//                            }
//                        }

                        if (docExtName.equalsIgnoreCase("Customer Photo")) {
                            docExtName = "c-" + strCif + "-pic";
                            dataMap.put("ProofName", docExtName);
                            dataMap.put("CsvName", "Customer Photo");
                            try (ByteArrayInputStream is = new ByteArrayInputStream(documentModule.getDocumentContent(sessionId, nodeD.getUuid(), versn))) {
                                status = MFfileOperation(is, dataMap, con);
                                System.out.println("[MF-PS]--FILE WAS WRITTEN SUCCESSFULLY FOR: " + dataMap.get("ProofName") + dataMap.get("PROCESSINSTANCEID"));
                            }

                            continue;

                        }
                        if (docExtName.equalsIgnoreCase("Signature")) {
                            docExtName = "c-" + strCif + "-sig";
                            dataMap.put("ProofName", docExtName);
                            dataMap.put("CsvName", "Customer Photo");
                            try (ByteArrayInputStream is = new ByteArrayInputStream(documentModule.getDocumentContent(this.sessionId, nodeD.getUuid(), versn))) {
                                status = MFfileOperation(is, dataMap, con);
                            }
                            System.out.println("[MF-PS]--FILE WAS WRITTEN SUCCESSFULLY FOR: " + dataMap.get("ProofName") + dataMap.get("PROCESSINSTANCEID"));
                        }
                    }
                    System.out.println("[MF-PS]-- All Doc Extracted Initiating C-Kyc");
                }
            } else {
                updateFlag(con, dataMap.get("PROCESSINSTANCEID"), "ERR", "SESSION NOT FOUND");
                System.out.println("[MF-PS]-- Session Not Found To Upload Docs");
            }
        } catch (InvalidRepositoryException | IOException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID"), "ERR", ex.getMessage());
            System.out.println("[MF-PS]-- EX -" + ex.getMessage());
            ex.printStackTrace();
            return status;
        }
        return status;
    }

    public int MFfileOperation(ByteArrayInputStream is, Map<String, String> dataMap, Connection con) {
        CSVWriter writer = null;
        int status = 0;
        try {
            String docName = "";
            String extension = "";
            //File file = new File("MFDMS_ARCHIVAL_PHOTO_SIG_NEW");
            File file = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG");
            if (!file.exists()) {
                boolean b = file.mkdirs();
                System.out.println("[MF DOC Upload] - Base Directory Created.");
            }
            String parentName = file.toString() + File.separator + "EXT-" + dataMap.get("CUSTOMERID");
            file = new File(parentName);
            this.configObj.setDmsSftpLocalDir("EXT-" + dataMap.get("CUSTOMERID"));
            dataMap.put("TEMPLOC", this.configObj.getDmsSftpLocalDir());
            if (!file.exists()) {
                file.mkdir();
                System.out.println("[MF-PS]-- Child Folder Created.");
            }
            docName = dataMap.get("DocName");
            extension = dataMap.get("EXT");
            System.out.println("[MF-PS]-- Extension found- " + extension);
            String docExtName = dataMap.get("ProofName");
            String DocTypeName = dataMap.get("CsvName");
            System.out.println("[MF-PS]-- File opertaion docExtName- " + docExtName);
            System.out.println("[MF-PS]-- File opertaion docExtName- " + DocTypeName);
            BufferedImage bImageFromConvert = ImageIO.read(is);
            //ImageIO.write(bImageFromConvert, extension, new File("MFDMS_ARCHIVAL_PHOTO_SIG_NEW" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension));
            ImageIO.write(bImageFromConvert, extension, new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG" + File.separator + "EXT-" + dataMap.get("CUSTOMERID") + File.separator + docExtName + "." + extension));
            System.out.println("[MF-PS]-- Doc Copied To Server Locations--" + DocTypeName);
            status = 1;
        } catch (IOException ex) {
            updateFlag(con, dataMap.get("PROCESSINSTANCEID"), "ERR", ex.getMessage());
            System.out.println("[MF-PS]--ex- " + ex);
            ex.printStackTrace();
            return status;
        }
        return status;
    }

    public Map DocUpload(Connection con, Map map) {
        ProfileIntg services = new ProfileIntg();
        RespBean SigresponseBean = new RespBean();
        RespBean PicresponseBean = new RespBean();
        ImageCompress compress = new ImageCompress();
        Map<String, String> Uploadmap = new HashMap<>();
        String UploadFlag = "false";
        String signFlag = "FAIL";
        String picFlag = "FAIL";
        Boolean SigFlag = false;
        Boolean PicFlag = false;
        String SigRemarks = "";
        String PicRemarks = "";
        try {
            String cifid = map.get("CIFID").toString();
            //File file = new File("MFDMS_ARCHIVAL_PHOTO_SIG_NEW" + File.separator + "EXT-" + cifid);
            File file = new File("DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG" + File.separator + "EXT-" + cifid);
            if (file.exists()) {
                System.out.println("[DOC RESPONSE]- Base Directory Created.File Name--" + file.getAbsolutePath());
//                String sig = System.getProperty("user.dir") + File.separator + "MFDMS_ARCHIVAL_PHOTO_SIG_NEW" + File.separator + "EXT-" + cifid + File.separator + "c-" + cifid + "-sig.jpg";
//                String pic = System.getProperty("user.dir") + File.separator + "MFDMS_ARCHIVAL_PHOTO_SIG_NEW" + File.separator + "EXT-" + cifid + File.separator + "c-" + cifid + "-pic.jpg";
//                String csig = System.getProperty("user.dir") + File.separator + "MFDMS_ARCHIVAL_PHOTO_SIG_NEW" + File.separator + "EXT-" + cifid + File.separator + "compress-" + cifid + "-sig.jpg";
//                String cpic = System.getProperty("user.dir") + File.separator + "MFDMS_ARCHIVAL_PHOTO_SIG_NEW" + File.separator + "EXT-" + cifid + File.separator + "compress-" + cifid + "-pic.jpg";
                String sig = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG" + File.separator + "EXT-" + cifid + File.separator + "c-" + cifid + "-sig.jpg";
                String pic = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG" + File.separator + "EXT-" + cifid + File.separator + "c-" + cifid + "-pic.jpg";
                String csig = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG" + File.separator + "EXT-" + cifid + File.separator + "compress-" + cifid + "-sig.jpg";
                String cpic = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PHOTO_SIG" + File.separator + "EXT-" + cifid + File.separator + "compress-" + cifid + "-pic.jpg";

                compress.picCompress(sig, csig, con, map);
                compress.picCompress(pic, cpic, con, map);
                File sigfile = new File(csig);
                File picfile = new File(cpic);
                String Sigbase = encodeFileToBase64Binary(sigfile);
                String Picbase = encodeFileToBase64Binary(picfile);
                StringBuffer sigrequestXml = signatureCall(cifid, Sigbase, map, con);
                StringBuffer picrequestXml = photoCall(cifid, Picbase, map, con);
                System.out.println("[MF-PS]--sigrequestXml- " + sigrequestXml);
                System.out.println("[MF-PS]--picrequestXml- " + picrequestXml);
                SigresponseBean = services.connectCall(sigrequestXml.toString(), this.configObj.getSigurl());
                PicresponseBean = services.connectCall(picrequestXml.toString(), this.configObj.getPicurl());
                System.out.println("[MF-PS]--SigResponse:" + SigresponseBean.getResponse());
                System.out.println("[MF-PS]--PicResponse:" + PicresponseBean.getResponse());
                if (SigresponseBean.getStatus() == 1) {
                    Map<String, Object> SigOutput = services.processProfileResponse(SigresponseBean.getResponse().toString());
                    Map<String, Object> PicOutput = services.processProfileResponse(PicresponseBean.getResponse().toString());
                    if (Integer.parseInt(SigOutput.get("status").toString()) == 1) {
                        System.out.println("[MF-PS]--Signature Upload Success");
                        System.out.println("[MF-PS]--signature message: " + SigOutput.get("message").toString());
                        SigFlag = true;
                        signFlag = "PASS";
                        SigRemarks = SigOutput.get("message").toString();
                    } else {
                        System.out.println("[MF-PS]--Signature Upload Fail");
                        System.out.println("[MF-PS]--Signature message: " + SigOutput.get("message").toString());
                        SigFlag = false;
                        signFlag = "FAIL";
                        SigRemarks = SigOutput.get("message").toString();
                    }
                    if (Integer.parseInt(PicOutput.get("status").toString()) == 1) {
                        System.out.println("[MF-PS]--Picture Upload Success");
                        System.out.println("[MF-PS]--picture message: " + PicOutput.get("message").toString());
                        PicFlag = true;
                        picFlag = "PASS";
                        PicRemarks = PicOutput.get("message").toString();
                    } else {
                        System.out.println("[MF-PS]--Picture Upload Fail");
                        System.out.println("[MF-PS]--picture message" + PicOutput.get("message").toString());
                        PicFlag = false;
                        picFlag = "FAIL";
                        PicRemarks = PicOutput.get("message").toString();
                    }
                }
            }
            if (SigFlag && PicFlag) {
                UploadFlag = "True";
            } else {
                UploadFlag = "False";
            }
            Uploadmap.put("UPLOADFLAG", UploadFlag);
            Uploadmap.put("SIGSTATUS", signFlag);
            Uploadmap.put("PICSTATUS", picFlag);
            Uploadmap.put("SIGMSG", SigRemarks);
            Uploadmap.put("PICMSG", PicRemarks);
        } catch (Exception e) {
            updateFlag(con, map.get("PROCESSINSTANCEID").toString(), "ERR", e.getMessage());
            System.out.println("Doc Download Exception:" + e.getMessage());
        }
        return Uploadmap;
    }

    private static String encodeFileToBase64Binary(File file) throws Exception {
        FileInputStream fileInputStreamReader = new FileInputStream(file);
        byte[] bytes = new byte[(int) file.length()];
        fileInputStreamReader.read(bytes);
        return new String(Base64.encodeBase64(bytes), "UTF-8");
    }

    public StringBuffer signatureCall(String cifId, String imgData, Map map, Connection con) {
        StringBuffer inputXml = new StringBuffer(100);
        try {
            inputXml.append(SRVUtil.startTag("input"));
            inputXml.append(SRVUtil.writeTag("Operation", "uploadSignature"));
            inputXml.append(SRVUtil.startTag("SessionContext"));
            inputXml.append(SRVUtil.writeTag("BranchCode", ""));
            inputXml.append(SRVUtil.writeTag("Channel", "SERVO"));
            inputXml.append(SRVUtil.writeTag("ExternalReferenceNo", cifId));
            inputXml.append(SRVUtil.startTag("SupervisorContext"));
            inputXml.append(SRVUtil.writeTag("UserId", "SERVO_MBIL"));
            inputXml.append(SRVUtil.writeTag("PrimaryPassword", "V2VsY29tZUAxMjM="));
            inputXml.append(SRVUtil.endTag("SupervisorContext"));
            inputXml.append(SRVUtil.endTag("SessionContext"));
            inputXml.append(SRVUtil.writeTag("CustomerID", cifId));
            inputXml.append(SRVUtil.writeTag("Signature", imgData));
            inputXml.append(SRVUtil.endTag("input"));
        } catch (Exception ex) {
            updateFlag(con, map.get("PROCESSINSTANCEID").toString(), "ERR", ex.getMessage());
            System.out.println("[ProfileEnrollmentCall]-{createCall}-EX-  " + ex);
        }
        return inputXml;
    }

    public StringBuffer photoCall(String cifId, String imgData, Map map, Connection con) {
        StringBuffer inputXml = new StringBuffer(100);
        try {
            inputXml.append(SRVUtil.startTag("input"));
            inputXml.append(SRVUtil.writeTag("Operation", "uploadPhoto"));
            inputXml.append(SRVUtil.startTag("SessionContext"));
            inputXml.append(SRVUtil.writeTag("BranchCode", ""));
            inputXml.append(SRVUtil.writeTag("Channel", "SERVO"));
            inputXml.append(SRVUtil.writeTag("ExternalReferenceNo", cifId));
            inputXml.append(SRVUtil.startTag("SupervisorContext"));
            inputXml.append(SRVUtil.writeTag("UserId", "SERVO_MBIL"));
            inputXml.append(SRVUtil.writeTag("PrimaryPassword", "V2VsY29tZUAxMjM="));
            inputXml.append(SRVUtil.endTag("SupervisorContext"));
            inputXml.append(SRVUtil.endTag("SessionContext"));
            inputXml.append(SRVUtil.writeTag("CustomerID", cifId));
            inputXml.append(SRVUtil.writeTag("Photo", imgData));
            inputXml.append(SRVUtil.endTag("input"));
        } catch (Exception ex) {
            updateFlag(con, map.get("PROCESSINSTANCEID").toString(), "ERR", ex.getMessage());
            System.out.println("[ProfileEnrollmentCall]-{createCall}-EX-  " + ex);
        }
        return inputXml;
    }

    public boolean updateFlag(Connection con, String pinstId, String type, String remarks) {
        String query = "";
        PreparedStatement pstmt = null;
        try {
            if (type.equalsIgnoreCase("UPD")) {
                query = "UPDATE MF_RU_DMSARCHIVAL SET EXECUTION_FLAG = ?,SIG_PIC_UPTIME=SYSDATE WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "I");
                pstmt.setString(2, "pinstId");
                pstmt.setString(2, pinstId);
            } else if (type.equalsIgnoreCase("ERR")) {
                query = "UPDATE MF_RU_DMSARCHIVAL SET EXECUTION_FLAG = ?,REMARKS = ? WHERE PROCESSINSTANCEID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, "PS_ERROR");
                pstmt.setString(2, remarks);
                pstmt.setString(3, pinstId);
            }
            pstmt.executeUpdate();
            closeConnection(null, pstmt);
        } catch (Exception ex) {
            System.out.println("[MF DOC Upload] -- UPdate Status Eex - " + ex);
            return Boolean.FALSE.booleanValue();
        } finally {
            closeConnection(null, pstmt);
        }
        return Boolean.TRUE.booleanValue();
    }

    public HashMap checkDMSSession(Connection con, int userId, propertyConfig config, String oldsessionId) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String query = "";
        int count = 0;
        SessionService service = new SessionService();
        HashMap<Object, Object> sessionMap = new HashMap<Object, Object>();
        try {
            query = "SELECT COUNT(SESSIONID) AS SESSIONCOUNT FROM SRV_RU_CONNECTIONINFO WHERE USERID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setInt(1, userId);
            rs = pstmt.executeQuery();
            rs.next();
            count = rs.getInt("SESSIONCOUNT");
            closeConnection(rs, pstmt);
            if (count == 0) {
                sessionMap = service.getSession(config.getIstreamsIp(), config.getIstreamsPort(), "MFDMSUSER1", "System1234#");
            }
            sessionMap.put("status", "1");
            sessionMap.put("sessionId", oldsessionId);
        } catch (Exception exception) {

        } finally {
            closeConnection(rs, pstmt);
        }
        return sessionMap;
    }
}
