/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.mbldoc;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.servo.businessdoc.SessionService;
import com.servo.config.SRVTimerServiceConfig;
import com.servo.configdoc.LoadConfig;
import com.servo.configdoc.propertyConfig;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Execute;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.TimerService;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
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
public class MBLBacklogSFTPResp {

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
            System.out.println("[MBL-BL]-- Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(this.configObj);
            System.out.println("[MBL-BL]--SftpIp - " + this.configObj.getDmsSftpIp());
            System.out.println("[MBL-BL]--SftpUserId - " + this.configObj.getDmsSftpUserId());
            System.out.println("[MBL-BL]--SftpDir - " + this.configObj.getDmsSftpDir());
            this.tokenMap = this.service.getSession(this.configObj.getIstreamsIp(), this.configObj.getIstreamsPort(), "DMS_USER", "system123#");
            if (this.tokenMap.get("status").equals("1")) {
                this.sessionId = this.tokenMap.get("sessionId").toString();
            }
            System.out.println("[MBL-BL]-- Post construct Executed Config Loaded For Doc Upload.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[MBL-BL]-- Config Load Exception->" + ex);
        }
    }

    @Execute
    public void execute(Connection con) throws Exception {
        System.out.println("[MBL-BL]--Inside execute Proceeding to Download from SFTP::::");
        initiateSftp(configObj, con);

    }

    public void initiateSftp(propertyConfig configObj, Connection con) throws JSchException {
        String remoteDirectory;
        StandardFileSystemManager manager = new StandardFileSystemManager();
        String serverAddress = configObj.getDmsSftpIp();
        String userId = configObj.getDmsSftpUserId();
        String password = configObj.getDmsSftpPswd();
        remoteDirectory = configObj.getBacklogsftpOut();
        // Remote directory
        //String remoteDirectory = "BACKLOG/BACKLOG1/OUT/";

        // Local directory
        String localDirectory = "DMS_ARCHIVAL/MBL/BACKLOG_RESPONSE/";

        System.out.println("[MBL-BL]--SFTP IP: " + serverAddress);
        System.out.println("[MBL-BL]--SFTP USERID: " + userId);
        System.out.println("[MBL-BL]--SFTP PWD: " + password);
        System.out.println("[MBL-BL]--REMOTE: " + remoteDirectory);
        System.out.println("[MBL-BL]--LOCAL: " + localDirectory);

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

            manager.init();
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);
            password = URLEncoder.encode(password);

            String sftpUri = "sftp://" + userId + ":" + password + "@" + serverAddress + "/" + remoteDirectory;

            System.out.println("[MBL-BL]--SFTP URL: " + sftpUri);

            FileObject remoteFile = manager.resolveFile(sftpUri, opts);

            // Get children (files) of the remote directory
            FileObject[] files = remoteFile.getChildren();

            // Copy a maximum of 50 files or all files if less than 50
            int filesToCopy = Math.min(files.length, 50);

            // Local directory file object
            FileObject localDir = manager.resolveFile(System.getProperty("user.dir") + File.separator + localDirectory);

            // Copy files from remote to local directory and delete remote files
            for (int i = 0; i < filesToCopy; i++) {
                FileObject file = files[i];
                // Construct local file path
                String localFilePath = localDirectory + file.getName().getBaseName();

                // Print local file path along with file name
                System.out.println("Local File Path To Copy The File: " + localFilePath);

                // Resolve local file object
                FileObject localFile = manager.resolveFile(localDir, file.getName().getBaseName());

                // Copy file from remote to local
                localFile.copyFrom(file, Selectors.SELECT_SELF);

                // Change permissions if necessary
                cSftp.chmod(511, remoteDirectory + file.getName().getBaseName());

                // Delete remote file
                file.delete();
            }
            System.out.println("[MBL-BL]-- DOWNLOAD IS SUCCESSFULL--PROCEEDING TO READ THE FILES.");
            processLocalFiles(localDirectory, con);

            // Updating Flag - Assuming this is where you want to update the flag
            // updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "");
        } catch (JSchException | SftpException | FileSystemException ex) {
            System.out.println("[MBL-BL]--Exception happened while copying form Sftp: " + ex);
            ex.printStackTrace();
            // Handle exceptions
        } catch (SQLException ex) {
            Logger.getLogger(MBLBacklogSFTPResp.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            // Close connections and resources
            if (manager != null) {
                manager.close();
            }
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    public void processLocalFiles(String localDirectory, Connection con) throws SQLException {
        try {
            // Local directory file object
            File localDir = new File(localDirectory);

            // Get list of files in the local directory
            File[] fileList = localDir.listFiles();

            // Process each file in the local directory
            for (File file : fileList) {

                String cifId = getCifIdWithoutExtension(file.getName());

                System.out.println("[MBL-BL]--FILE TO BE READ: " + file.getName());

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

                        System.out.println("[MBL-BL]-- DATA IN THE SECOND LINE: " + dataPartscontents);

                        System.out.println("[MBL-BL]--LENGTH OF THE dataParts : " + dataParts.length);

                        if (dataParts.length >= 1) {
                            status = dataParts[0];
                        }
                        if (dataParts.length == 2) {
                            reason = dataParts[1];
                        }
                    }

                    System.out.println("[MBL-BL]--STATUS: " + status + " REASON: " + reason + "FOR THE CIF: " + cifId);
                }

                // Update database based on STATUS and file name (CIFID)
                //String cifId = file.getName(); // Assuming file name is CIFID
                boolean updateStatus;
                if ("success".equalsIgnoreCase(status)) {
                    // Update database status as "YES" and clear remarks
                    updateStatus = updateDatabase(con, cifId, "YES", "SUCCESS RESPONSE RECIEVED");
                } else {
                    // Update database status as "NO" and set remarks as reason
                    updateStatus = updateDatabase(con, cifId, "NO", reason);
                }
                System.out.println("[MBL-BL]--TABLE UPDATE STATUS: " + updateStatus);

                if (updateStatus) {
                    System.out.println("[MBL-BL]-- STATUS IS UPDATED IN THE TABLE SUCCESFULLY FOR THE CIF: " + cifId);
                    Files.deleteIfExists(file.toPath());
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public boolean updateDatabase(Connection con, String cifId, String status, String remarks) throws SQLException {
        PreparedStatement stmt = null;
        boolean success = false;
        try {
            // Prepare SQL statement
            String sql = "UPDATE MBL_BACKLOG_SFTP SET Status = ?, Remarks = ? WHERE Cifid = ?";
            stmt = con.prepareStatement(sql);
            stmt.setString(1, status);
            stmt.setString(2, remarks);
            stmt.setString(3, cifId);
            // Execute update
            int rowsAffected = stmt.executeUpdate();
            // Check if the update was successful
            if (rowsAffected > 0) {
                success = true;
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return success;
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
