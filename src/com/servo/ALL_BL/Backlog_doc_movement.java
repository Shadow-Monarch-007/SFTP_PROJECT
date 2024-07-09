/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.ALL_BL;

import au.com.bytecode.opencsv.CSVWriter;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.servo.businessdoc.SessionService;
import com.servo.configdoc.LoadConfig;
import com.servo.configdoc.propertyConfig;
import static com.servo.mfdoc.MFCkycUpload_0Kb_version.ImageToPDF;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
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
import javax.annotation.TimerService;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import oracle.jdbc.OracleTypes;
import org.apache.commons.vfs.*;
import org.apache.commons.vfs.impl.StandardFileSystemManager;
import org.apache.commons.vfs.provider.sftp.SftpFileSystemConfigBuilder;

/**
 *
 * @author VINITH
 */
@TimerService
public class Backlog_doc_movement {

    propertyConfig configObj = new propertyConfig();
    SessionService service = new SessionService();
    HashMap tokenMap = new HashMap();
    HashMap Cif_Datamap;
    String sessionId = "";
    Set<String> foundDocument;
    String available_docs;

    @PostConstruct
    public void loadConfigurations(Connection con1) throws NoSuchProviderException, MessagingException {
        try {
            System.out.println("[BL-DOC-CHECK]-- Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(configObj);

            System.out.println("[BL-DOC-CHECK]--SftpIp - " + configObj.getDmsSftpIp());
            System.out.println("[BL-DOC-CHECK]--SftpUserId - " + configObj.getDmsSftpUserId());
            System.out.println("[BL-DOC-CHECK]--BaclogIn - " + configObj.getBacklogsftpIn());
            System.out.println("[BL-DOC-CHECK]--62KCIF_DIR - " + configObj.getBacklogsftpIn());
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

        //CallableStatement callableStatement = null;
        String getDMSUploadSql = "{call CKYC_DOC_MOVE(?)}";
        //Map HashMap = null;

        try {
            try (CallableStatement callableStatement = con.prepareCall(getDMSUploadSql)) {
                callableStatement.registerOutParameter(1, OracleTypes.CURSOR);
                callableStatement.executeUpdate();
                try (ResultSet rs = (ResultSet) callableStatement.getObject(1)) {
                    while (rs.next()) {
                        foundDocument = new HashSet<>();
                        Cif_Datamap = new HashMap<>();
                        available_docs = null;
                        String CIFID = rs.getString("CIFID");
                        String ACC_NO = rs.getString("ACCOUNTNO");
                        Cif_Datamap.put("CIFID", CIFID);
                        Cif_Datamap.put("ACC_NO", ACC_NO);
                        System.out.println("[BL-DOC-MOVE]- CIF TO BE PROCESSED: " + CIFID);
                        //READ_DIR(CIFID, con);
                        int cif_move_status = MOVE_CIF(Cif_Datamap, con);

                        switch (cif_move_status) {
                            case 1: {
                                System.out.println("[BL-DOC-MOVE]--CIF DOCUMENTS MOVED TO SFTP: " + CIFID);
                            }
                            case 0: {
                                System.out.println("[BL-DOC-MOVE]--CIF DOCUMENTS NOT MOVED TO SFTP: " + CIFID);
                            }
                        }

                    }
                }
            }

        } catch (SQLException e) {
            System.out.println("[BL-DOC-MOVE]--EX: " + e.getMessage());

        }
        //closeCallableConnection(callableStatement);

    }

    public int MOVE_CIF(Map dataMap, Connection con) {
        System.out.println("[BL-DOC-MOVE]--PROCEEDING TO PROCESS THE DOCUMENTS:");
        String CIFID = dataMap.get("CIFID").toString();
        System.out.println("[BL-DOC-MOVE]--CIFID: " + CIFID);
        //String ACC_NO = dataMap.get("ACC_NO").toString();
        //System.out.println("[BL-DOC-MOVE]--ACC_NO: " + ACC_NO);

        int status = 0;

        Path sourceBaseDir = Paths.get("D:\\62K_Record");
        System.out.println("[BL-DOC-MOVE]--sourceBaseDir: " + sourceBaseDir.toString());
        Path targetBaseDir = Paths.get("DMS_ARCHIVAL" + File.separator + "READY_OUTOF_62K");
        System.out.println("[BL-DOC-MOVE]--targetBaseDir: " + targetBaseDir.toString());
        try {
            Path sourceDir = sourceBaseDir.resolve(CIFID);
            Path targetDir = targetBaseDir.resolve("EXT-" + CIFID);

            if (Files.exists(targetDir)) {
                Files.walkFileTree(targetDir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }

            // Ensure the target directory exists
            if (!Files.exists(targetDir)) {
                Files.createDirectories(targetDir);
            }

            Files.walk(sourceDir).forEach(sourcePath -> {
                try {
                    Path relativePath = sourceDir.relativize(sourcePath);
                    Path targetPath = targetDir.resolve(relativePath);

                    // Extract base name and extension
                    String fileName = relativePath.getFileName().toString();
                    String baseName = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
                    String extension = fileName.contains(".") ? fileName.substring(fileName.lastIndexOf('.')) : "";

                    System.out.println("[BL-DOC-MOVE]--FILENAME: " + baseName);
                    System.out.println("[BL-DOC-MOVE]--EXTENSION: " + extension);

                    // Rename files according to the specified rules
                    if (baseName.equalsIgnoreCase("CUSTOMER PHOTO")
                            || baseName.equalsIgnoreCase("MBL Customer Photo")
                            || baseName.equalsIgnoreCase("FIG CUSTOMER PHOTO")) {
                        targetPath = targetDir.resolve(relativePath.getParent()).resolve("Photograph" + extension);
                    } else if (baseName.equalsIgnoreCase("SIGNATURE OR THUMB IMPRESSION")
                            || baseName.equalsIgnoreCase("MBL Signature or Thumb")
                            || baseName.equalsIgnoreCase("FIG SIGNATURE OR THUMB IMPRESSION")) {
                        targetPath = targetDir.resolve(relativePath.getParent()).resolve("Signature" + extension);
                    }

                    if (Files.isDirectory(sourcePath)) {
                        if (!Files.exists(targetPath)) {
                            Files.createDirectory(targetPath);
                        }
                    } else {
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                        System.out.println("[BL-DOC-MOVE]--FILES COPIED SUCCESFULLY: " + targetPath.toString());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            // Ensure the target directory exists

            System.out.println("Files copied successfully!");
            String csvFilePath = targetDir.toString() + File.separator + "EXT-" + CIFID + ".csv";
            //String csvFilePath = targetDir.toString() + File.separator + "EXT-" + CIFID + ".csv";
            //String csvFilePath = file.toString() + File.separator + "EXT-" + CIFID + ".csv";
            File csvFile = new File(csvFilePath);
            if (!csvFile.exists()) {
                try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile), '|', CSVWriter.NO_QUOTE_CHARACTER)) {
                    String[] header = "FILENAME,DC.WINAME,DC.ACCNO,DC.CIFNO,DOCTYPE".split(",");
                    writer.writeNext(header);

                    String[] values = ("," + "," + "1000000000" + "," + CIFID + "|,").split(",");
                    writer.writeNext(values);
                }
                System.out.println("[BL-DOC-MOVE]-- - Child Folder and CSV Created.");
            }

            try {
                status = cKycupdate(con, dataMap);
                System.out.println("[BL-DOC-MOVE]-- CKYC Update Done");
            } catch (Exception ex) {

                dataMap.put("REMARKS", ex.getMessage());
                updateFlag(con, dataMap, "EXC");

            }

        } catch (IOException ex) {
            dataMap.put("REMARKS", ex.getMessage());
            updateFlag(con, dataMap, "EXC");
            ex.printStackTrace();
            return status;
        }

        return status;

    }

    public int cKycupdate(Connection con, Map dataMap) {

        int status = 0;

        String fileA = "";
        String fileB = "";
        String csvName = "";
        String proofName = "";

        String StrVal = "Address Proof,ID Proof,Address proof Corress,Relation ID Proof,Second ID Proof";

        String[] arrSplit = StrVal.split(",");

        Path targetDir = Paths.get("D:\\READY_OUTOF_62K", "EXT-" + dataMap.get("CIFID").toString());
        List<String> files = new ArrayList<>();
        try {

            for (String strTemp : arrSplit) {
                System.out.println("BL-DOC-MOVE]--PROCEEDING TO MERGE DOCUMENTS: " + strTemp);

                if (strTemp.equalsIgnoreCase("ID Proof")) {
                    System.out.println("[BL-DOC-MOVE]--MERGING ID PROOF");
                    fileA = targetDir + File.separator + "ID Proof Front.jpg";
                    fileB = targetDir + File.separator + "ID Proof Back.jpg";
                    csvName = strTemp;
                    proofName = "UID.pdf";
                } else if (strTemp.equalsIgnoreCase("Address Proof") || strTemp.equalsIgnoreCase("Address proof Corress") || strTemp.equalsIgnoreCase("Relation ID Proof") || strTemp.equalsIgnoreCase("Second ID Proof")) {
                    System.out.println("[BL-DOC-MOVE]--MERGING: " + strTemp);
                    fileA = targetDir + File.separator + strTemp + " Front.jpg";
                    fileB = targetDir + File.separator + strTemp + " Back.jpg";
                    csvName = strTemp;
                    proofName = strTemp + ".pdf";
                }

                File file_front = new File(fileA);

                File file_back = new File(fileB);

                if (file_front.exists() && file_back.exists()) {
                    files.add(fileA);
                    files.add(fileB);
                    System.out.println("[BL-DOC-MOVE]---BOTH FRONT AND BACK ID PROOFS ARE VAILABLE: ");
                    System.out.println("[BL-DOC-MOVE]---fileA--" + fileA);
                    System.out.println("[BL-DOC-MOVE]---fileB--" + fileB);
                    System.out.println("[BL-DOC-MOVE]--Pdf upload csvname: " + csvName);
                    ImageToPDF(files, targetDir + File.separator + proofName);

                    files.clear();
                    try {
                        Files.deleteIfExists(targetDir.resolve("ID Proof Front.jpg"));
                        Files.deleteIfExists(targetDir.resolve("ID Proof Back.jpg"));
                        System.out.println("Files deleted successfully.");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else if (!file_front.exists() && !file_back.exists()) {
                    // Neither file exists
                    System.out.println("[BL-DOC-MOVE]---NEITHER FRONT NOR BACK ID PROOFS ARE AVAILABLE.");
                    // Handle the case where neither file exists, if needed
                } else if (file_front.exists() && !file_back.exists()) {
                    System.out.println("[BL-DOC-MOVE]---BACK ID PROOF IS NOT AVAILABLE.");

                } else if (!file_front.exists() && file_back.exists()) {
                    System.out.println("[BL-DOC-MOVE]---FRONT ID PROOF IS NOT AVAILABLE.");

                }

            }
            System.out.println("[BL-DOC-MOVE]---PROCEEDING FOR SFTP: ");
            //Path dir = Paths.get("D:\\62K_Record");
//            String folder_to_read = dir.toString() + File.separator + dataMap.get("CIFID").toString();
//            File directory = new File(folder_to_read);

            try {
                String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "READY_OUTOF_62K" + File.separator + "EXT-" + dataMap.get("CIFID").toString();
                System.out.println("[BL-DOC-MOVE]--DIRECTORY TO READ: " + "DMS_ARCHIVAL" + File.separator + "READY_OUTOF_62K" + File.separator + "EXT-" + dataMap.get("CIFID").toString());
                File directory = new File(deletedirectoryPath);
                System.out.println("[BL-DOC-MOVE]--1");

                if (directory.exists() && directory.isDirectory()) {
                    System.out.println("[BL-DOC-MOVE]--2");
                    // List the files in the directory
                    File[] written_files = directory.listFiles();
                    System.out.println("[BL-DOC-MOVE]--3");
                    if (written_files != null) {
                        System.out.println("[BL-DOC-MOVE]--4");
                        // Iterate over the files
                        for (File foundFile : written_files) {
                            System.out.println("[BL-DOC-MOVE]--5");
                            //fileNameWithoutExtension = removeExtension(foundFile.getName());
                            foundDocument.add(foundFile.getName());
                            //foundDocument.add(fileNameWithoutExtension);
                        }
                        available_docs = String.join(", ", foundDocument);
                        System.out.println("[BL-DOC-MOVE]--6");

                    } else {
                        System.err.println("[BL-DOC-MOVE]--Error: Unable to list files in directory.");
                    }
                } else {
                    System.err.println("[BL-DOC-MOVE]--Error: Directory does not exist or is not a directory.");
                }
                status = initiateSftp(configObj, con, dataMap);
//                if (updateFlag(con, dataMap, "UD")) {
//                    
//                }
            } catch (Exception e) {
                System.out.println("[BL-DOC-MOVE]-- PRESFTP Ex- " + e);
                dataMap.put("REMARKS", e.getMessage());
                System.out.println("Updating File ");
                updateFlag(con, dataMap, "EXC");
                return status;
            }

        } catch (Exception fx) {
            System.out.println("[BL-DOC-MOVE]-- Ex- " + fx);
            dataMap.put("REMARKS", fx.getMessage());
            System.out.println("Updating File ");
            updateFlag(con, dataMap, "EXC");
            return status;

        }
        return status;
    }

    public int initiateSftp(propertyConfig configObj, Connection con, Map dataMap) {
        int status = 0;
        String remoteDirectory;
        StandardFileSystemManager manager = new StandardFileSystemManager();
        String serverAddress = configObj.getDmsSftpIp();
        String userId = configObj.getDmsSftpUserId();
        String password = configObj.getDmsSftpPswd();
        remoteDirectory = configObj.getBacklogsftpIn();

        System.out.println("[MF SFTP IP]--" + serverAddress);
        System.out.println("[MF SFTP USERID]--" + userId);
        System.out.println("[MF SFTP PASSWORD]--" + password);

        //PROD
        //remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/IN24/";
        //UAT
        //remoteDirectory = "BACKLOG/BACKLOG1/IN/";
        System.out.println("[MF SFTP REMOTE DIRECTORY]--" + remoteDirectory);
        //String localDirectory = "DMS_ARCHIVAL/NEW/";

        String localDirectory = "DMS_ARCHIVAL/READY_OUTOF_62K/";
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
                fileToFTP = "EXT-"+ dataMap.get("CIFID").toString();
            }
            System.out.println("[BL-DOC-MOVE]-- Local Dir -" + localDirectory + fileToFTP);

            System.out.println("[BL-DOC-MOVE]-- check if the file exists");
            String filepath = localDirectory + fileToFTP;
            File file = new File(filepath);
            if (!file.exists()) {
                throw new RuntimeException("[BL-DOC-MOVE]--MF MERGE  Error. Local file not found");
            }

            System.out.println("[BL-DOC-MOVE]-- Initializes the file manager");
            manager.init();

            System.out.println("[BL-DOC-MOVE]-- Setup our SFTP configuration");
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(
                    opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

            password = URLEncoder.encode(password);

            System.out.println("[BL-DOC-MOVE]-- Create the SFTP URI using the host name, userid, password,  remote path and file name");
            String sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                    + remoteDirectory + fileToFTP;

            System.out.println("[BL-DOC-MOVE]-- " + sftpUri);

            System.out.println("[BL-DOC-MOVE]-- Create local file object");
            System.out.println("[BL-DOC-MOVE]-- CKyc-- File-" + file.getAbsolutePath());
            FileObject localFile = manager.resolveFile(file.getAbsolutePath());

            System.out.println("[BL-DOC-MOVE]-- Create remote file object");
            FileObject remoteFile = manager.resolveFile(sftpUri, opts);

            System.out.println("[BL-DOC-MOVE]-- Copy local file to sftp server");
            remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
            System.out.println("[BL-DOC-MOVE]-- Folder upload successful");

            File localDir = new File(localDirectory + fileToFTP);
            File[] subFiles = localDir.listFiles();
            for (File item : subFiles) {
                sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
                        + remoteDirectory + fileToFTP + File.separator + item.getName();

                file = new File(localDirectory + fileToFTP + File.separator + item.getName());
                System.out.println("[BL-DOC-MOVE]-- " + file.getAbsolutePath());
                localFile = manager.resolveFile(file.getAbsolutePath());
                remoteFile = manager.resolveFile(sftpUri, opts);
                if (item.isFile()) {
                    remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                    System.out.println("MF remoteDirectory:" + remoteDirectory);
                    System.out.println("MF remoteDirectory remain:" + fileToFTP + File.separator + item.getName());
                    System.out.println("MF remoteDirectory fileToFTP:" + fileToFTP);
                    System.out.println("MF remoteDirectory item :" + item.getName());
                    cSftp.chmod(511, remoteDirectory + fileToFTP + "/" + item.getName());
                    System.out.println("[MF-SFTP-BL]-- File upload successful");
                }
            }
            cSftp.chmod(511, remoteDirectory + "/" + "EXT-" + dataMap.get("CIFID").toString());
            dataMap.put("REMARKS", "FILES MOVED TO SFTP");
            System.out.println("[BL-DOC-MOVE]-- Updating Flag.");
            if (updateFlag(con, dataMap, "UD")) {

                String deletecustomerID = dataMap.get("CIFID").toString();
                String deletedirectoryPath = "DMS_ARCHIVAL" + File.separator + "READY_OUTOF_62K";
                String folderToDelete = "EXT-" + deletecustomerID;

                File directoryToDelete = new File(deletedirectoryPath, folderToDelete);

                if (directoryToDelete.exists()) {

                    System.out.println("[MF-SFTP-BL]--Directory Found -- Deleting Existing Folder After upload to sftp: " + "EXT-" + deletecustomerID);
                    // Delete the directory
                    deleteDirectory(directoryToDelete);
                } else {
                    // Directory doesn't exist
                    System.out.println("[MF-SFTP-BL]--Directory does not exist.");
                }
                

            }
        } catch (JSchException | SftpException | RuntimeException | FileSystemException ex) {
            status = 0;
            dataMap.put("REMARKS", ex.getMessage());
            updateFlag(con, dataMap, "EXC");
            System.out.println("[MF-SFTP-BL]-- SFTP Ex - " + ex);
        } finally {
            // CH-D-2001
            manager.close();
            channel.disconnect();
            session.disconnect();

            //end
        }
        return status;
    }

//    public int initiateSftp(propertyConfig configObj, Connection con, Map dataMap) throws IOException {
//        int status = 0;
//        String remoteDirectory;
//        String localDirectory;
//        StandardFileSystemManager manager = new StandardFileSystemManager();
//        String serverAddress = configObj.getDmsSftpIp();
//        String userId = configObj.getDmsSftpUserId();
//        String password = configObj.getDmsSftpPswd();
//        remoteDirectory = configObj.getBacklogsftpIn();
//
//        System.out.println("[MF SFTP IP]--" + serverAddress);
//        System.out.println("[MF SFTP USERID]--" + userId);
//        System.out.println("[MF SFTP PASSWORD]--" + password);
//
//        //PROD
//        //remoteDirectory = "SERVOSYS/BPMDOCUPLOAD/IN24/";
//        //UAT
//        //remoteDirectory = "BACKLOG/BACKLOG1/IN/";
//        localDirectory = "DMS_ARCHIVAL/READY_OUTOF_62K/";
//
//        //String localDirectory = "DMS_ARCHIVAL/MF/BACKLOG_SFTP/";
//        Session session = null;
//        Channel channel = null;
//        try {
//
//            //Path targetBaseDir = Paths.get("D:\\READY_OUTOF_62K");
//            //Path targetDir = targetBaseDir.resolve("EXT-" + dataMap.get("CIFID").toString());
//            //String localDirectory = targetDir.toString();
//            System.out.println("[BL-DOC-MOVE]--remote: " + remoteDirectory);
//            System.out.println("[BL-DOC-MOVE]--local: " + localDirectory);
//            String fileToFTP = "EXT-" + dataMap.get("CIFID");
//
//            JSch jsch = new JSch();
//            session = jsch.getSession(userId, serverAddress, 22);
//            session.setPassword(password);
//            session.setConfig("StrictHostKeyChecking", "no");
//
//            session.connect();
//
//            channel = session.openChannel("sftp");
//            channel.connect();
//            ChannelSftp cSftp = (ChannelSftp) channel;
//
////            if (fileToFTP == null) {
////                fileToFTP = dataMap.get("TEMPDOC_LOCATION").toString();
////            }
//            System.out.println("[BL-DOC-MOVE] - Local Dir -" + localDirectory + fileToFTP);
//
//            System.out.println("[BL-DOC-MOVE] - check if the file exists");
//            String filepath = localDirectory + fileToFTP;
//            File file = new File(filepath);
//            if (!file.exists()) {
//                throw new RuntimeException("MF MERGE  Error. Local file not found");
//            }
//
//            System.out.println("[BL-DOC-MOVE] - Initializes the file manager");
//            manager.init();
//
//            System.out.println("[BL-DOC-MOVE] - Setup our SFTP configuration");
//            FileSystemOptions opts = new FileSystemOptions();
//            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(
//                    opts, "no");
//            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
//            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);
//
//            password = URLEncoder.encode(password);
//
//            System.out.println("[BL-DOC-MOVE] - Create the SFTP URI using the host name, userid, password,  remote path and file name");
//            String sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
//                    + remoteDirectory + fileToFTP;
//
//            System.out.println("[BL-DOC-MOVE]-- " + sftpUri);
//
//            System.out.println("[BL-DOC-MOVE]-- Create local file object");
//            FileObject localFile = manager.resolveFile(file.getAbsolutePath());
//
//            System.out.println("[BL-DOC-MOVE]-- Create remote file object");
//            FileObject remoteFile = manager.resolveFile(sftpUri, opts);
//
//            System.out.println("[BL-DOC-MOVE]-- Copy local file to sftp server");
//            remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
//            System.out.println("[BL-DOC-MOVE]-- Folder upload successful");
//
//            File localDir = new File(localDirectory + fileToFTP);
//            File[] subFiles = localDir.listFiles();
//            for (File item : subFiles) {
//                sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/"
//                        + remoteDirectory + fileToFTP + File.separator + item.getName();
//
//                file = new File(localDirectory + fileToFTP + File.separator + item.getName());
//                System.out.println("[BL-DOC-MOVE]-- " + file.getAbsolutePath());
//                localFile = manager.resolveFile(file.getAbsolutePath());
//                remoteFile = manager.resolveFile(sftpUri, opts);
//                if (item.isFile()) {
//                    remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
//                    System.out.println("MF remoteDirectory:" + remoteDirectory);
//                    System.out.println("MF remoteDirectory remain:" + fileToFTP + File.separator + item.getName());
//                    System.out.println("MF remoteDirectory fileToFTP:" + fileToFTP);
//                    System.out.println("MF remoteDirectory item :" + item.getName());
//                    cSftp.chmod(511, remoteDirectory + fileToFTP + "/" + item.getName());
//                    System.out.println("[BL-DOC-MOVE]-- File upload successful");
//                }
//            }
//            cSftp.chmod(511, remoteDirectory + "/" + "EXT-" + dataMap.get("CUSTOMERID").toString());
//            System.out.println("[BL-DOC-MOVE]-- Updating Flag.");
//            dataMap.put("REMARKS", "FILES MOVED TO SFTP");
//            if (updateFlag(con, dataMap, "UD")) {
//                status = 1;
//
//                System.out.println("[BL-DOC-MOVE] -- Routing Done for - " + dataMap.get("CIFID").toString());
//
////                Path targetBaseDir = Paths.get("D:\\READY_OUTOF_62K");
////                Path targetDir = targetBaseDir.resolve("EXT-" + dataMap.get("CIFID").toString());
////                if (Files.exists(targetDir)) {
////                    Files.walkFileTree(targetDir, new SimpleFileVisitor<Path>() {
////                        @Override
////                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
////                            Files.delete(file);
////                            return FileVisitResult.CONTINUE;
////                        }
////
////                        @Override
////                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
////                            Files.delete(dir);
////                            return FileVisitResult.CONTINUE;
////                        }
////                    });
////                }
//            }
//            return status;
//        } catch (JSchException | SftpException | RuntimeException | FileSystemException ex) {
//            dataMap.put("REMARKS", ex.getMessage());
//            updateFlag(con, dataMap, "EXC");
//            System.out.println("[BL-DOC-MOVE]-- SFTP Ex - " + ex);
//            return status;
//        } finally {
//            // CH-D-2001
//            manager.close();
//            channel.disconnect();
//            session.disconnect();
//            //end
//        }
//
//    }
    public boolean updateFlag(Connection con, Map Hashmap, String flag) {
        String query;
        PreparedStatement pstmt = null;
        System.out.println("[BL-DOC-MOVE]--available_docs: " + available_docs);
        System.out.println("[BL-DOC-MOVE]--Remarks: " + Hashmap.get("REMARKS").toString());
        System.out.println("[BL-DOC-MOVE]--flag: " + flag);
        System.out.println("[BL-DOC-MOVE]--CIFID: " + Hashmap.get("CIFID").toString());
        

        try {

            query = "UPDATE ALL_CIF_DOC_CHECK SET DOCS_IN_SFTP=?,REMARKS=?,EXECUTION_FLAG=?,PROCESSED_AT=SYSDATE WHERE CIFID =?";
            try (PreparedStatement pstate = con.prepareStatement(query)) {
                pstate.setString(1, available_docs);
                pstate.setString(2, Hashmap.get("REMARKS").toString());
                pstate.setString(3, flag);
                pstate.setString(4, Hashmap.get("CIFID").toString());
                pstmt.executeUpdate();
                return Boolean.TRUE;
            }
        } catch (SQLException ex) {
            System.out.println("[DOC Upload] -- UPdate Status Eex - " + ex);
            return Boolean.FALSE;
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

}
