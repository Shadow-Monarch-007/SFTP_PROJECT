package com.servo.businessdoc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

public class SessionService {

    public HashMap getSession(String istreamsIP, String port, String UserName, String pswd) {
//        String UserName = "DMS_USER";
//        String pswd = "system123#";
        String response = "";
        String str = "";
        String sessionID = "";
        String userId = "";
        HashMap tokenMap = new HashMap();
        try {
            URL url = new URL("http://" + istreamsIP + ":" + port + "/istreams/LoginWS");
            URLConnection conn = url.openConnection();
            conn.setDoOutput(true);

            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()));
            out.write("username=" + UserName + "&password=" + pswd + "&isForceFull=Y");
            out.flush();
            out.close();

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while ((response = in.readLine()) != null) {
                str = str + response;

            }
            System.out.println("data -- login-->" + str);
            XmlParser parser = new XmlParser();
            parser.setInputXML(str);
            sessionID = parser.getValueOf("sessionid");
            userId = parser.getValueOf("userid");
            if (sessionID.equals("")) {
                tokenMap.put("status", "0");
            } else {
                tokenMap.put("status", "1");
                tokenMap.put("sessionId", sessionID);
            }
            if (userId.equals("")) {
                tokenMap.put("userId", "0");
            } else {
                tokenMap.put("userId", userId);
            }
            in.close();
        } catch (Exception ex) {
            tokenMap.put("status", "0");
            tokenMap.put("error", ex.getMessage());
            return tokenMap;
        }
        return tokenMap;
    }

}
