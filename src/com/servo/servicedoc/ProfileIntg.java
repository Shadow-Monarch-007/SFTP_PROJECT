/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.servicedoc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.servo.beandoc.RespBean;
import com.servo.configdoc.HttpsTrustManager;
import java.net.URL;

/**
 *
 * @author VINITH
 */
public class ProfileIntg {
  public RespBean connectCall(String inputXml, String PrfUrl) {
    StringBuilder result = new StringBuilder();
    RespBean response = new RespBean();
    StringBuilder sb = new StringBuilder();
    try {
      HttpsTrustManager.allowAllSSL();
      URL url = new URL(PrfUrl);
      HttpURLConnection con = (HttpURLConnection)url.openConnection();
      System.out.println("URL-- " + url);
      con.setDoInput(true);
      con.setDoOutput(true);
      con.setRequestMethod("POST");
      con.setConnectTimeout(50000);
      con.setReadTimeout(50000);
      con.setUseCaches(false);
      con.setDefaultUseCaches(false);
      con.setRequestProperty("Content-Type", "application/xml");
      OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream());
      writer.write(inputXml);
      writer.flush();
      writer.close();
      InputStreamReader reader = new InputStreamReader(con.getInputStream());
      StringBuilder buf = new StringBuilder();
      char[] cbuf = new char[2048];
      int num;
      while (-1 != (num = reader.read(cbuf)))
        buf.append(cbuf, 0, num); 
      result = buf;
      response.setStatus(Integer.valueOf(1));
      response.setResponse(result);
      System.err.println("ProfileServices Result:" + result);
    } catch (Throwable t) {
      response.setStatus(Integer.valueOf(0));
      response.setResponse(sb.append(t.getMessage()));
      System.out.println("ProfileServices Exception --" + t);
    } 
    return response;
  }
  
  public Map<String, Object> processProfileResponse(String responseXml) {
    String xmlToken = null;
    String PRAN_Number = null;
    HashMap<Object, Object> outputMap = new HashMap<>();
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      ByteArrayInputStream input = new ByteArrayInputStream(responseXml.getBytes("UTF-8"));
      Document doc = dBuilder.parse(input);
      NodeList nodes = doc.getElementsByTagName("Exception");
      int nodeLength = nodes.getLength();
      if (nodeLength == 1) {
        Node nNode = nodes.item(0);
        Element eElement = (Element)nNode;
        xmlToken = eElement.getElementsByTagName("ErrorMessage").item(0).getTextContent().trim();
        System.out.println("ErrorMessage->" + xmlToken);
        outputMap.put("status", 0);
        outputMap.put("message", xmlToken);
      } else {
        nodes = doc.getElementsByTagName("Response");
        Node nNode = nodes.item(0);
        Element eElement = (Element)nNode;
        PRAN_Number = eElement.getElementsByTagName("ReplyCode").item(0).getTextContent();
        xmlToken = eElement.getElementsByTagName("ReplyText").item(0).getTextContent();
        System.out.println("ReplyCode->" + PRAN_Number);
        System.out.println("ReplyText->" + xmlToken);
        outputMap.put("status", Integer.valueOf(1));
        outputMap.put("ReplyCode", PRAN_Number);
        outputMap.put("message", xmlToken);
      } 
    } catch (ParserConfigurationException ex) {
      outputMap.put("status", Integer.valueOf(0));
      outputMap.put("message", ex.getMessage());
      System.out.println("[processProfileResponse]--{processOutput}-Exception->" + ex);
      return (Map)outputMap;
    } catch (SAXException | IOException | DOMException ex) {
      outputMap.put("status", Integer.valueOf(0));
      outputMap.put("message", ex.getMessage());
      System.out.println("[processProfileResponse]--{processOutput}-Exception->" + ex);
      return (Map)outputMap;
    } catch (Exception ex) {
      outputMap.put("status", Integer.valueOf(0));
      outputMap.put("message", ex.getMessage());
      System.out.println("[processProfileResponse]--{processOutput}-Exception->" + ex);
      return (Map)outputMap;
    } 
    return (Map)outputMap;
  }
}
