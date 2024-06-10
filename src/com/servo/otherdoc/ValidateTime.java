/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.otherdoc;

import com.servo.beandoc.ExceptionBean;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author user
 */
public class ValidateTime {

    public static boolean init(ExceptionBean bean) {
//        String initTime = "07:00";
//        String endTime = "18:00";
        String secondsSystime;
        String secondsintime;
        String secondsouttime;
        try {
            String systime[] = sysTime().split(":");
            String intim[] = bean.getInitTime().split(":");
            String outtim[] = bean.getEndTime().split(":");
            secondsSystime = String.valueOf((Integer.valueOf(systime[0]) * 3600) + (Integer.parseInt(systime[1]) * 60));
            secondsintime = String.valueOf((Integer.valueOf(intim[0]) * 3600) + (Integer.parseInt(intim[1]) * 60));
            secondsouttime = String.valueOf((Integer.valueOf(outtim[0]) * 3600) + (Integer.parseInt(outtim[1]) * 60));
            boolean dateValid = Integer.parseInt(secondsintime) <= Integer.parseInt(secondsSystime) && Integer.parseInt(secondsSystime) <= Integer.parseInt(secondsouttime);
            System.out.println("Is It Right time to get Branch Data : " + dateValid);
            return dateValid;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static String sysTime() {
        String date = "";
        try {
            long yourmilliseconds = System.currentTimeMillis();
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
            Date resultdate = new Date(yourmilliseconds);
            date = sdf.format(resultdate);
            System.out.println("Time system - " + date);
        } catch (Exception e) {
        }
        return date;
    }
}
