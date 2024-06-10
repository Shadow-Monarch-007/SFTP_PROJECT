package com.servo.configdoc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LoadConfig {

    public void readConfig(propertyConfig config) {

        Properties prop = new Properties();
        InputStream input = null;
        try {
            System.out.println("INSIDE LOAD CONFIG");
            input = new FileInputStream("SRVConfig/Properties/FIGServices.properties");
            prop.load(input);
            config.setBacklogsftpIn(prop.getProperty("backlogsftpIn"));
            config.setBacklogsftpOut(prop.getProperty("backlogsftpOut"));         
            
            config.setProcessid(Integer.parseInt(prop.get("processId").toString()));
            config.setDedupeActivityId(Integer.parseInt(prop.get("dedupeActivityId").toString()));
            config.setAmlReqActivityId(Integer.parseInt(prop.get("amlReqActivityId").toString()));
            config.setAmlRespActivityId(Integer.parseInt(prop.get("amlRespActivityId").toString()));
            config.setCbActivityId(Integer.parseInt(prop.get("cbActivityId").toString()));
            config.setUserId(Integer.parseInt(prop.get("userId").toString()));
            config.setPgKitURL(prop.getProperty("pgKit"));
            config.setDedupeURL(prop.getProperty("dedupe"));
            config.setInstancePoolingCount(Integer.parseInt(prop.getProperty("instancePoolingCount")));
            config.setBrUrl(prop.getProperty("brNet"));
            config.setAppId(prop.getProperty("appId"));
            config.setClientName(prop.getProperty("clientName"));
            config.setPresharedKey(prop.getProperty("presharedKey"));
            config.setIsProfileActive(prop.getProperty("activeProfile"));
            config.setIstreamsIp(prop.getProperty("istreamsIP"));
            config.setIstreamsPort(prop.getProperty("istreamsPort"));
            config.setDmsIp(prop.getProperty("dmsIP"));
            config.setDmsPort(prop.getProperty("dmsPort"));
            config.setClientCreationActid(Integer.parseInt(prop.getProperty("clientCreationActivityId")));
            config.setDmsSftpIp(prop.getProperty("dmsSftpIp"));
            config.setDmsSftpUserId(prop.getProperty("dmsSftpUserId"));
            config.setDmsSftpPswd(prop.getProperty("dmsSftpPswd"));
            config.setDmsSftpDir(prop.getProperty("dmsSftpDir"));
            config.setSbSftpDir(prop.getProperty("sbSftpDir"));
            config.setDmsSftpDirOut(prop.getProperty("dmsSftpDirOut"));
            config.setRlosSftpDirOut(prop.getProperty("rlosSftpDirOut"));
            config.setUserName(prop.getProperty("userName"));
            config.setSbReqActId(Integer.parseInt(prop.getProperty("SbReqActId")));
            config.setSbRespActId(Integer.parseInt(prop.getProperty("SbRespActId")));
            config.setSbInitTime(prop.getProperty("SbInitTime"));
            config.setSbEndTime(prop.getProperty("SbEndTime"));
//            config.setDiscardId(Integer.parseInt(prop.get("DISCARDID").toString()));
            config.setCbUser1(Integer.parseInt(prop.get("FIGCB1").toString()));
            config.setCbUser2(Integer.parseInt(prop.get("FIGCB2").toString()));
            config.setCbUser3(Integer.parseInt(prop.get("FIGCB3").toString()));
            config.setCbUser4(Integer.parseInt(prop.get("FIGCB4").toString()));
            config.setCbUser5(Integer.parseInt(prop.get("FIGCB5").toString()));
            config.setCbValidity(Integer.valueOf(prop.getProperty("cbValidity")));
            //   config.setPadedupeActivityId(Integer.valueOf(prop.getProperty("PA_dedupeActivityId")));
            // config.setPacbActivityId(Integer.valueOf(prop.getProperty("PA_cbActivityId")));
            // config.setPaloancreationActid(Integer.valueOf(prop.getProperty("PA_LoanCreationActivityId")));
            // config.setPacbUser1(Integer.parseInt(prop.get("PA_CB1").toString()));
            //config.setPacbUser2(Integer.parseInt(prop.get("PA_CB2").toString()));
            config.setSigurl(prop.getProperty("SIGURL"));
            config.setPicurl(prop.getProperty("PICURL"));
            config.setDedupeUser1(Integer.parseInt(prop.get("FIGDEDUPE1").toString()));
            config.setDedupeUser2(Integer.parseInt(prop.get("FIGDEDUPE2").toString()));
            config.setDedupeUser3(Integer.parseInt(prop.get("FIGDEDUPE3").toString()));
            config.setLSUSER1(Integer.parseInt(prop.get("FIGLoanSanction1").toString()));
            config.setLSUSER2(Integer.parseInt(prop.get("FIGLoanSanction2").toString()));
            config.setLSUSER3(Integer.parseInt(prop.get("FIGLoanSanction3").toString()));
            config.setFetchUrl(prop.getProperty("FetchUrl"));
            config.setVaultKey(prop.getProperty("adharVaultEncKey"));
            config.setPgnActivityId(Integer.parseInt(prop.getProperty("PGNActivityId")));
            config.setPgnUrl(prop.getProperty("PROFILEURL"));
            config.setProfileUrlCustomer(prop.get("profileUrlCustomer").toString());
            config.setCifCreationActivityId(Integer.parseInt(prop.getProperty("CIFCreationActivityId")));
            config.setAvsUpdateURL(prop.getProperty("UpdateUrl"));
            config.setLocalDirectry(prop.getProperty("CKYCLOCALDIR"));
            config.setNON_SERVO_CIF_DIR(prop.getProperty("NON_SERVO_CIF_DIR"));
            config.setCKYC_RESP_DEL(prop.getProperty("CKYC_RESP_DEL"));
            config.setCKYC_READ_DIR(prop.getProperty("CKYC_READ_DIR"));


            System.out.println("CONFIG loaded--");
            System.out.println(config.getProcessid());
        } catch (IOException ex) {
            ex.printStackTrace();
            System.out.println("Read Property file Exception-->" + ex);
        } catch (Exception e) {
            System.out.println("Broad Exception-->" + e);
            e.printStackTrace();

        }
    }
}
