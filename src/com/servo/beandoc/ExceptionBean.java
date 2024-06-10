/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.beandoc;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author SRV014
 */
public class ExceptionBean {

    private long sessionId = 0L;
    private int userId = 0;
    private String initTime;
    private String endTime;

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getInitTime() {
        return initTime;
    }

    public void setInitTime(String initTime) {
        this.initTime = initTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public static ExceptionBean initializeExceptionBean() {
        Properties prop = new Properties();
        InputStream input = null;
        ExceptionBean bean = new ExceptionBean();
        try {
            System.out.println("INSIDE LOAD CONFIG");
            input = new FileInputStream("SRVConfig/Properties/submitException.properties");
            prop.load(input);
            bean.setSessionId(Long.parseLong(prop.get("submitExceptionSessionId").toString()));
            bean.setUserId(Integer.parseInt(prop.get("submitExceptionUserId").toString()));
            bean.setInitTime(prop.get("submitExceptionInitTime").toString());
            bean.setEndTime(prop.get("submitExceptionEndTime").toString());
            input.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Read Property file Exception-->" + ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Exception ex) {
                    
                }
            }
        }
        return bean;
    }
}
