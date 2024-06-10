/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.ALLCKYC;

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
import java.net.URLEncoder;
import java.sql.Connection;
import java.util.HashMap;
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
/**
 *
 * @author VINITH
 */
@TimerService
public class Allckycresponse_downloader {

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
            System.out.println("[ALLCKYC-RESP]--BacklogOut - " + this.configObj.getBacklogsftpOut());

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
        System.out.println("[ALLCKYC-RESP]--Inside execute Proceeding to Download from SFTP::::");
        initiateSftp(configObj, con);

    }

    public void initiateSftp(propertyConfig configObj, Connection con) throws JSchException {
        String remoteDirectory;
        StandardFileSystemManager manager = new StandardFileSystemManager();
        String serverAddress = configObj.getDmsSftpIp();
        String userId = configObj.getDmsSftpUserId();
        String password = configObj.getDmsSftpPswd();
        remoteDirectory = configObj.getDmsSftpDirOut();

        // Remote directory
        //String remoteDirectory = "BACKLOG/BACKLOG1/OUT/";
        // Local directory
        String localDirectory = "DMS_ARCHIVAL/ALLCKYC_RESPONSE/";
//        String localDirectory = "DMS_ARCHIVAL/MF/ALLCKYC_RESPONSE/";

        System.out.println("[ALLCKYC-RESP]--SFTP IP: " + serverAddress);
        System.out.println("[ALLCKYC-RESP]--SFTP USERID: " + userId);
        System.out.println("[ALLCKYC-RESP]--SFTP PWD: " + password);
        System.out.println("[ALLCKYC-RESP]--REMOTE: " + remoteDirectory);
        System.out.println("[ALLCKYC-RESP]--LOCAL: " + localDirectory);

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

            System.out.println("[ALLCKYC-RESP]--SFTP URL: " + sftpUri);

            FileObject remoteFile = manager.resolveFile(sftpUri, opts);

            // Get children (files) of the remote directory
            FileObject[] files = remoteFile.getChildren();

            // Copy a maximum of 50 files or all files if less than 50
            int filesToCopy = Math.min(files.length, 200);

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
            System.out.println("[ALLCKYC-RESP]-- DOWNLOAD IS SUCCESSFULL--PROCEEDING TO FOR NEXT BATCH OF THE FILES.");
            //processLocalFiles(localDirectory, con);

            // Updating Flag - Assuming this is where you want to update the flag
            // updateFlag(con, dataMap.get("PROCESSINSTANCEID").toString(), "UPD", "");
        } catch (JSchException | SftpException | FileSystemException ex) {
            System.out.println("[ALLCKYC-RESP]--Exception happened while copying form Sftp: " + ex);
            ex.printStackTrace();
            // Handle exceptions
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

}
