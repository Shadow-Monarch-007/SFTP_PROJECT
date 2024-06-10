/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.ALLCKYC;

import com.servo.businessdoc.SessionService;
import com.servo.config.SRVTimerServiceConfig;
import com.servo.configdoc.LoadConfig;
import com.servo.configdoc.propertyConfig;
import com.servo.db_con.DBUtil;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Scanner;
import javax.annotation.Execute;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.TimerService;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author VINITH
 */
@TimerService
public class Allckycresponse_updater {

    propertyConfig configObj = new propertyConfig();
    String initTime = "";
    static String errorFlag = "";
    @Resource
    private SRVTimerServiceConfig Timer_Service_Id;

    HashMap tokenMap = new HashMap<>();

    String sessionId = "";

    SessionService service = new SessionService();

    @PostConstruct
    public void loadConfigurations(Connection con1) throws NoSuchProviderException, MessagingException {
        try {
            System.out.println("[ALLCKYC-RESP]-- Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(this.configObj);
            System.out.println("[ALLCKYC-RESP]--SftpIp - " + this.configObj.getDmsSftpIp());
            System.out.println("[ALLCKYC-RESP]--SftpUserId - " + this.configObj.getDmsSftpUserId());
            System.out.println("[ALLCKYC-RESP]--SftpDir - " + this.configObj.getDmsSftpDir());
            System.out.println("[ALLCKYC-RESP]--BacklogOut - " + this.configObj.getDmsSftpDirOut());
            System.out.println("[ALLCKYC-RESP]--LocalDir - " + this.configObj.getLocalDirectry());
            System.out.println("[ALLCKYC-RESP]--OtherCIFDir - " + this.configObj.getNON_SERVO_CIF_DIR());
            System.out.println("[ALLCKYC-RESP]--Ckys_read_Dir - " + this.configObj.getCKYC_READ_DIR());

            this.tokenMap = this.service.getSession(this.configObj.getIstreamsIp(), this.configObj.getIstreamsPort(), "DMS_USER", "system123#");
            if (this.tokenMap.get("status").equals("1")) {
                this.sessionId = this.tokenMap.get("sessionId").toString();
            }
            System.out.println("[ALLCKYC-RESP]-- Post construct Executed Config Loaded For Doc Upload.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ALLCKYC-RESP]-- Config Load Exception->" + ex);
        }
    }

    @Execute
    public void execute(Connection con) throws Exception {
        System.out.println("[ALLCKYC-RESP]--Inside execute Proceeding to Update ckyc status from Local directory::::");

        String LOCALDIR = this.configObj.getLocalDirectry();

        System.out.println("[ALLCKYC-RESP]--LOCAL DIR: " + LOCALDIR);

        processLocalFiles(LOCALDIR, con);

    }

    public void processLocalFiles(String localDirectory, Connection con) throws SQLException {
        try {
            // Local directory file object
            File localDir = new File(localDirectory);

            // Get list of files in the local directory
            File[] fileList = localDir.listFiles();

            if (fileList == null) {
                System.out.println("The specified directory is empty or not a directory.");
                return;
            }

            // Sort files by last modified time in ascending order
            Arrays.sort(fileList, Comparator.comparingLong(File::lastModified));

            // Process only the first 200 files
            int filesToProcess = Math.min(fileList.length, 200);

//            for (int i = 0; i < filesToProcess; i++) {
//                File file = fileList[i];
//                // Your logic to process each file
//                processFile(file, con);
//            }
            // Process each file in the local directory
            //for (File file : fileList) {
            for (int i = 0; i < filesToProcess; i++) {
                File file = fileList[i];
                String cifId = getCifIdWithoutExtension(file.getName());

                System.out.println("[ALLCKYC-RESP]--FILE TO BE READ: " + file.getName());

                String status;
                String reason;
                try (Scanner scanner = new Scanner(file)) {
                    String line;
                    status = "";
                    reason = "";
                    // Skip the first line (headers)
                    if (scanner.hasNextLine()) {
                        scanner.nextLine(); // Assuming headers are present and not needed for processing
                    }

                    // Read the second line (data)
                    if (scanner.hasNextLine()) {
                        String[] dataParts = scanner.nextLine().split("\\|");

                        String dataPartscontents = Arrays.toString(dataParts);

                        System.out.println("[ALLCKYC-RESP]-- DATA IN THE SECOND LINE: " + dataPartscontents);

                        System.out.println("[ALLCKYC-RESP]--LENGTH OF THE dataParts : " + dataParts.length);

                        if (dataParts.length >= 1) {
                            status = dataParts[0];
                        }
                        if (dataParts.length == 2) {
                            reason = dataParts[1];
                        }
                    }

                    System.out.println("[ALLCKYC-RESP]--STATUS: " + status + " REASON: " + reason + "FOR THE CIF: " + cifId);
                }

                // Update database based on STATUS and file name (CIFID)
                //String cifId = file.getName(); // Assuming file name is CIFID
                String[] tables = {"MF_RU_DMSARCHIVAL", "FIG_RU_DMSARCHIVAL", "MBL_RU_DMSARCHIVAL", "CASA_RU_DMSARCHIVAL"};

                boolean updateStatus = false;
                if ("success".equalsIgnoreCase(status)) {
                    // Update database status as "YES" and clear remarks
                    //updateStatus = updateDatabase(con, cifId, "YES", "SUCCESS RESPONSE RECIEVED");

                    for (String table : tables) {
                        if (table.equals("CASA_RU_DMSARCHIVAL")) {
                            try (Connection CASAconnection = DBUtil.getCASAConnection()) {
                                updateStatus = updateStatus(CASAconnection, cifId, status, "SUCCESS RESPONSE RECIEVED", table);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        } else {
                            updateStatus = updateStatus(con, cifId, status, reason, table);
                        }
                        if (updateStatus) {
                            System.out.println("[ALLCKYC-RESP]-- STATUS IS UPDATED IN THE TABLE SUCCESFULLY FOR THE CIF: " + cifId + " BREAKING OUT OF LOOP.");
                            break;
                        }
                    }
                } else {
                    // Update database status as "NO" and set remarks as reason
                    //updateStatus = updateDatabase(con, cifId, "NO", reason);

                    for (String table : tables) {
                        if (table.equals("CASA_RU_DMSARCHIVAL")) {
                            try (Connection CASAconnection = DBUtil.getCASAConnection()) {
                                updateStatus = updateStatus(CASAconnection, cifId, status, reason, table);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        } else {
                            updateStatus = updateStatus(con, cifId, status, reason, table);
                        }
                        if (updateStatus) {
                            System.out.println("[ALLCKYC-RESP]-- STATUS IS UPDATED IN THE TABLE SUCCESFULLY FOR THE CIF: " + cifId + " BREAKING OUT OF LOOP.");
                            break;
                        }
                    }
                }
                System.out.println("[ALLCKYC-RESP]--TABLE UPDATE STATUS: " + updateStatus);

                String d_status = this.configObj.getCKYC_RESP_DEL();

                if (updateStatus) {
                    System.out.println("[ALLCKYC-RESP]-- STATUS IS UPDATED IN THE TABLE SUCCESFULLY FOR THE CIF: " + cifId);
                    switch (d_status) {

                        case "Y": {

                            System.out.println("[ALLCKYC-RESP]----CKYC RESPONSE DELETE STATUS IS SET AS YES --DELETING RESP FOR CIF: " + cifId);
                            Files.deleteIfExists(file.toPath());
                            break;
                        }
                        case "N": {
                            System.out.println("[ALLCKYC-RESP]--CKYC RESPONSE DELETE STATUS IS SET AS NO -- MOVING THE CKYC RESPONSE.");
                            moveFile(configObj.getCKYC_READ_DIR(), cifId);
                            break;
                        }
                        default: {

                            System.out.println("[ALLCKYC-RESP]----CKYC RESPONSE DELETE STATUS IS SET AS YES --DELETING RESP FOR CIF: " + cifId);
                            Files.deleteIfExists(file.toPath());
                            break;
                        }

                    }

                } else {
                    System.out.println("[ALLCKYC-RESP]-- CIFID DOESN'T BELONG TO SERVOSYS: " + cifId);
                    System.out.println("[ALLCKYC-RESP]-- MOVING THE CIF TO NEW LOCATION:");
                    moveFile(configObj.getNON_SERVO_CIF_DIR(), cifId);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void moveFile(String targetDirPath, String cifId) {
        // Construct the source file path
        String sourceFilePath = configObj.getLocalDirectry() + File.separator + "EXT-" + cifId + ".txt";
        // Construct the target directory path
        //String targetDirPath = configObj.getNON_SERVO_CIF_DIR();

        LocalDateTime now = LocalDateTime.now();
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH;mm;ss");
        String timestamp = now.format(formatter);
        // Construct the destination file path
        String destinationFilePath = targetDirPath + File.separator + "EXT-" + cifId + "-" + timestamp + ".txt";

        System.out.println("[ALLCKYC-RESP]--SOURCE FILE PATH: " + sourceFilePath);
        System.out.println("[ALLCKYC-RESP]--DESINATION FILE PATH: " + destinationFilePath);

        Path sourcePath = Paths.get(sourceFilePath);
        Path destinationPath = Paths.get(destinationFilePath);

        try {
            // Copy the file to the destination directory
            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
            // Delete the original file
            Files.delete(sourcePath);
            System.out.println("[ALLCKYC-RESP]--File moved successfully.");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("[ALLCKYC-RESP]--Error occurred while moving the file.");
        }
    }

    public boolean updateStatus(Connection con, String cifId, String status, String remarks, String tableName) {
        System.out.println("[ALLCKYC-RESP]--STATUS FROM RESPONSE: " + status);
        System.out.println("[ALLCKYC-RESP]--REMARKS FROM RESPONSE: " + remarks);
        System.out.println("[ALLCKYC-RESP]--CIFID TO BE UPDATED: " + cifId);

        String query = "SELECT COUNT(processinstanceid) AS CASE_COUNT FROM " + tableName + " WHERE CIFID = ?";
//        String query = "SELECT COUNT(processinstanceid) AS CASE_COUNT FROM " + tableName + " WHERE STATUS IS NULL AND CIFID = ?";
        try (PreparedStatement pstmt = con.prepareStatement(query)) {
            pstmt.setString(1, cifId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    int work_item = rs.getInt("CASE_COUNT");
                    if (work_item > 0) {
                        String updateSql = "UPDATE " + tableName + " SET Status = ?, Remarks = ? ,CKYC_RESP_TIME = SYSDATE WHERE Cifid = ?";
                        try (PreparedStatement updStmt = con.prepareStatement(updateSql)) {
                            updStmt.setString(1, status);
                            updStmt.setString(2, remarks);
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
                        System.out.println("[ALLCKYC-RESP]--CIF: " + cifId + " DOESN'T BELONG TO " + tableName);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("[ALLCKYC-RESP]--EXCEPTIKON OCCURED WHILE TRYING TO UPDATE THE TABLE: " + tableName);
            e.printStackTrace();
        }
        return false;
    }

    private String getCifIdWithoutExtension(String fileName) {
        // Remove the "EXT-" prefix
        String cifId = fileName.replaceFirst("^EXT-", "");
        // Remove the extension
        int lastIndex = cifId.lastIndexOf('.');
        if (lastIndex > 0) {
            cifId = cifId.substring(0, lastIndex);
        }
        return cifId;
    }

//    private String getCifIdWithoutExtension(String fileName) {
//        int lastIndex = fileName.lastIndexOf('.');
//        if (lastIndex > 0) {
//            return fileName.substring(0, lastIndex);
//        } else {
//            return fileName;
//        }
//    }
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

}
