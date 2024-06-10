/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.businessdoc;

import com.servo.otherdoc.ByteStreamSecurity;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author SRV022
 */
public class DmsModule {

    ByteStreamSecurity byteStreamSecurity = new ByteStreamSecurity();

    public String getDocumentPath(Connection con, String pinstId, String fileNameTosave, String FileExpectedName, String loaclPath) {
        InputStream documentContent = null;
        String path = null;
        StringBuilder path_sb = new StringBuilder();
        String storagePath = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        String relativePath = null;
        Long uuid = 0L;
        String fileExt = null;
        String FileName = "";
        try {
            Long deviceID = 0L;

            preparedStatement = con.prepareStatement("select * from SDM_NODE_DOCUMENT where upper(NBS_NAME) =upper(?) and \n"
                    + " nbs_parent = (select folderid from srv_ru_execution where processinstanceid=? and rownum<=1)");

            preparedStatement.setString(1, fileNameTosave);
            preparedStatement.setString(2, pinstId);
            rs = preparedStatement.executeQuery();

            if (rs.next()) {

                relativePath = rs.getString("NDV_CURRENT");
                deviceID = rs.getLong("NBS_VOLUME");
                fileExt = rs.getString("NDC_EXT");
            }
            closeConnection(rs, preparedStatement);

            preparedStatement = con.prepareStatement("select  STORAGEPATH  from SDM_DEVICE where DVC_SITE=1 and DVC_VOLUME=?");
            preparedStatement.setLong(1, deviceID);
            rs = preparedStatement.executeQuery();

            if (rs.next()) {

                storagePath = rs.getString("STORAGEPATH");
            }
            closeConnection(rs, preparedStatement);
            System.out.println("deviceID : " + deviceID);
            System.out.println("storagePath : " + storagePath);

//            System.out.println(resolveRelativePath(relativePath));
            path_sb.append(resolveRelativePath(relativePath));
            path = storagePath + File.separator + path_sb.toString();

            documentContent = new FileInputStream(path);
            System.out.println("content is : " + documentContent);

            System.out.println("\n\t ******* Invoke genrateAuditTrail method ******\n");
//            fileByteArray = 
            FileOutputStream fos = new FileOutputStream(loaclPath + File.separator + resolveAbsoluteFileName(con, FileExpectedName, pinstId) + "." + fileExt);
            fos.write(byteStreamSecurity.byteInputStreamDecryption(IOUtils.toByteArray(documentContent)));
            fos.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            closeConnection(rs, preparedStatement);
        }

        return path;
    }

    public String resolveAbsoluteFileName(Connection con, String relativeFileName, String pinstId) {
        Map<String, String> mapString = new HashMap<String, String>();
        Pattern p = Pattern.compile("\\{(.*?)\\}");
        Matcher m = p.matcher(relativeFileName);
        while (m.find()) {
            mapString.put(m.group(1), "");
        }
        if (mapString.isEmpty()) {
            return relativeFileName;
        }
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String columnRequired = "";
            for (Map.Entry<String, String> entry : mapString.entrySet()) {
                columnRequired = entry.getKey() + ",";
            }

            columnRequired = columnRequired.substring(0, columnRequired.length() - 1);

            preparedStatement = con.prepareStatement("select " + columnRequired + " from MF_EXT where pinstid = ?");
            preparedStatement.setString(1, pinstId);
            rs = preparedStatement.executeQuery();

            if (rs.next()) {
                for (String columnName : columnRequired.split(",")) {
                    mapString.put(columnName, rs.getString(columnName));
                }
            } else {
//                throw new PathNotFoundException("UUDI : " + uuid);
            }
            closeConnection(rs, preparedStatement);
            for (Map.Entry<String, String> entry : mapString.entrySet()) {
                relativeFileName = relativeFileName.replace("{" + entry.getKey() + "}", entry.getValue());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            closeConnection(rs, preparedStatement);
        }

        return relativeFileName;
    }

    public String resolveRelativePath(String versionUuid) {
        char[] seq = versionUuid.replaceAll("-", "").toCharArray();
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < 8; i = i + 2) {
            path.append(seq[i]).append(seq[i + 1]).append(File.separator);
            System.out.println("Path of resolve Path :" + path);
        }
        return path.toString() + versionUuid;
    }

    private void closeConnection(ResultSet rs, PreparedStatement ps) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (ps != null) {
                ps.close();
                ps = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
