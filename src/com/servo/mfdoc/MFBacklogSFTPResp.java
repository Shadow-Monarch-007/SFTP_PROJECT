/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.mfdoc;

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
public class MFBacklogSFTPResp {

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
            System.out.println("[MF-BK]-- Inside Post Construct Loading Config.");
            LoadConfig objConfig = new LoadConfig();
            objConfig.readConfig(this.configObj);
            System.out.println("[MF-BK]--SftpIp - " + this.configObj.getDmsSftpIp());
            System.out.println("[MF-BK]--SftpUserId - " + this.configObj.getDmsSftpUserId());
            System.out.println("[MF-BK]--SftpDir - " + this.configObj.getDmsSftpDir());
            System.out.println("[MF-BK]--BacklogOut - " + this.configObj.getBacklogsftpOut());
            
            
            this.tokenMap = this.service.getSession(this.configObj.getIstreamsIp(), this.configObj.getIstreamsPort(), "DMS_USER", "system123#");
            if (this.tokenMap.get("status").equals("1")) {
                this.sessionId = this.tokenMap.get("sessionId").toString();
            }
            System.out.println("[MF-BK]-- Post construct Executed Config Loaded For Doc Upload.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[MF-BK]-- Config Load Exception->" + ex);
        }
    }

    @Execute
    public void execute(Connection con) throws Exception {
        System.out.println("[MF-BK]--Inside execute Proceeding to Download from SFTP::::");
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
        String localDirectory = "DMS_ARCHIVAL/MF/BACKLOG_RESPONSE/";

        System.out.println("[MF-BL]--SFTP IP: " + serverAddress);
        System.out.println("[MF-BL]--SFTP USERID: " + userId);
        System.out.println("[MF-BL]--SFTP PWD: " + password);
        System.out.println("[MF-BL]--REMOTE: " + remoteDirectory);
        System.out.println("[MF-BL]--LOCAL: " + localDirectory);

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

            System.out.println("[MF-BL]--SFTP URL: " + sftpUri);

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
            System.out.println("[MF-BL]-- DOWNLOAD IS SUCCESSFULL--PROCEEDING TO READ THE FILES.");
            processLocalFiles(localDirectory, con);

            // Updating Flag - Assuming this is where you want to update the flag
            // updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "");
        } catch (JSchException | SftpException | FileSystemException ex) {
            System.out.println("[MF-BL]--Exception happened while copying form Sftp: " + ex);
            ex.printStackTrace();
            // Handle exceptions
        } catch (SQLException ex) {
            Logger.getLogger(MFBacklogSFTPResp.class.getName()).log(Level.SEVERE, null, ex);
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

                System.out.println("[MF-BL]--FILE TO BE READ: " + file.getName());

                String status;
                String reason;
                try (Scanner scanner = new Scanner(file)) {
                    String line;
                    status = "";
                    reason = "SUCCESS RESPONSE RECIEVED";
                    // Skip the first line (headers)
                    if (scanner.hasNextLine()) {
                        scanner.nextLine(); // Assuming headers are present and not needed for processing
                    }

                    // Read the second line (data)
                    if (scanner.hasNextLine()) {
                        String[] dataParts = scanner.nextLine().split("\\|");

                        String dataPartscontents = Arrays.toString(dataParts);

                        System.out.println("[MF-BL]-- DATA IN THE SECOND LINE: " + dataPartscontents);

                        System.out.println("[MF-BL]--LENGTH OF THE dataParts : " + dataParts.length);

                        if (dataParts.length >= 1) {
                            status = dataParts[0];
                        }
                        if (dataParts.length == 2) {
                            reason = dataParts[1];
                        }
                    }

                    System.out.println("[MF-BL]--STATUS: " + status + " REASON: " + reason + "FOR THE CIF: " + cifId);
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
                System.out.println("[MF-BL]--TABLE UPDATE STATUS: " + updateStatus);

                if (updateStatus) {
                    System.out.println("[MF-BL]-- STATUS IS UPDATED IN THE TABLE SUCCESFULLY FOR THE CIF: " + cifId);
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
            System.out.println("[MF-BL]--STATUS FROM RESPONSE: " + status);
            System.out.println("[MF-BL]--REMARKS FROM RESPONSE: " + remarks);
            System.out.println("[MF-BL]--CIFID TO BE UPDATED: " + cifId);
            // Prepare SQL statement
            String sql = "UPDATE MF_BACKLOG_SFTP SET Status = ?, Remarks = ? WHERE Cifid = ?";
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
        } catch (Exception e) {
            System.out.println("[MF-BL]--ERROR WHILE UPLOADING MF_BACKLOG_SFTP: " + e);

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
