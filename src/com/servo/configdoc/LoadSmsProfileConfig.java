/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.configdoc;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;
import org.json.JSONObject;

/**
 *
 * @author SRV012
 */
public class LoadSmsProfileConfig {

    public static void readConfig(smsProfileConfig config) {

        Properties prop = new Properties();
        InputStream input = null;
        try {
            System.out.println("INSIDE LOAD CONFIG");

            input = new FileInputStream("SRVConfig" + File.separator
                    + "Properties" + File.separator + "SMSProperty" + File.separator + "smsProfileDetails.properties");
            prop.load(input);

            config.setAppId(prop.getProperty("appId"));
            config.setUserId(prop.getProperty("userId"));
            config.setPwd(prop.getProperty("pwd"));
            config.setOperation(prop.getProperty("operation"));
            config.setTemplateId(prop.getProperty("templateId"));

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Read Property file Exception-->" + ex);
        }
    }

    public static void readMessages(JSONObject messagesObject) {
        try {
            File messages = new File("SRVConfig" + File.separator
                    + "Properties" + File.separator + "SMSProperty" + File.separator + "FIGMessages");
            for (final File fileEntry : messages.listFiles()) {
                InputStream input = null;
                input = new FileInputStream(
                        fileEntry);
                Properties prop = new Properties();
                prop.load(input);
                input.close();
                JSONObject keyValuePair = new JSONObject();
                Enumeration enumLanguageProperty = prop.propertyNames();
                while (enumLanguageProperty.hasMoreElements()) {
                    String key = (String) enumLanguageProperty.nextElement();
                    String value = prop.getProperty(key);
                    keyValuePair.put(key, value);
}
                messagesObject.put(fileEntry.getName().replace(".properties", ""), keyValuePair);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
