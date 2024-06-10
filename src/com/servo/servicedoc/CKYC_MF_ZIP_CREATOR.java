/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.servicedoc;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Execute;
import javax.annotation.PostConstruct;
import javax.annotation.TimerService;

/**
 *
 * @author Durai
 */
@TimerService
public class CKYC_MF_ZIP_CREATOR {

//    @Resource
//    private SRVTimerServiceConfig Timer_Service_Id;
    @PostConstruct
    public void loadConfigurations(Connection con1) {
        System.out.println("CKYC_MF_ZIP_CREATOR Loaded successfully");
    }

    public String fetchSequence(Connection con) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String query = "";
        int count = 0;
        String seq = "0";
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");

            String date = formatter.format(new Date());
            System.out.println(date);
            query = "SELECT COUNT(1) AS COUNT FROM CKYC_SEQ_LIST WHERE SEQ_DATE = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, date);

            rs = pstmt.executeQuery();
            rs.next();

            count = rs.getInt("COUNT");
            if (count == 0) {
                Statement stmt = con.createStatement();
                String sql = "drop SEQUENCE ckyc_extract_sequence_seq";
                stmt.executeUpdate(sql);
                System.out.println("SEQUENCE DROPPED ");
                sql = "CREATE SEQUENCE ckyc_extract_sequence_seq";
                stmt.executeUpdate(sql);
                System.out.println("SEQUENCE RECREATED ");
                sql = "INSERT INTO CKYC_SEQ_LIST (SEQ_CREATED,SEQ_DATE) VALUES(?,?)";
                pstmt = con.prepareStatement(sql);
                pstmt.setString(1, "Y");
                pstmt.setString(2, date);
                pstmt.execute();
                seq = generateSeq(con);
            } else {
                seq = generateSeq(con);
            }
            System.out.println(count);

        } catch (Exception ex) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            ex.printStackTrace();
        }
        return seq;
    }

    public String generateSeq(Connection con) {
        System.out.println("INSIDE GENERATE SEQ");
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String query = "";
        String seq = "0";
        try {
            query = "select lpad ( ckyc_extract_sequence_seq.nextval, 5, '0' ) id from dual";
            pstmt = con.prepareStatement(query);

            rs = pstmt.executeQuery();
            rs.next();

            seq = rs.getString(1);

        } catch (Exception e) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            e.printStackTrace();
        }

        return seq;
    }

//    public static void main(String[] args) {
//        try {
//
//            String jdbcUrl = "jdbc:postgresql://localhost:5432/SERVODB";
//            String username = "postgres";
//            String password = "root";
//
//            // Register the PostgreSQL driver
//            Class.forName("org.postgresql.Driver");
//
//            // Connect to the database
//            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
//            System.out.println("Connection check --" + connection);
//            execute(connection);
//            // Perform desired database operations
//            // Close the connection
//            connection.close();
//        } catch (Exception ex) {
//
//            ex.printStackTrace();
//        }
//    }
    @Execute
    public void execute(Connection con) throws Exception {
        try {
            System.out.println("INSIDE EXECUTE MF_CKYC" + con);

            List<HashMap<String, Object>> cifData = getZipCIFData(con);
            System.out.println("OUTSIDE EXECUTE getZipCIFData");
            if (cifData.size() > 0) {
                System.out.println("INSIDE EXECUTE cifData size");
                int totalSize = 0;
                String finalDataForBody = "";
                boolean folderCreationStatus = false;
                System.out.println("INSIDE EXECUTE fetchSequence size");
                String seqNo = fetchSequence(con);
                System.out.println("OUTSIDE EXECUTE fetchSequence size");
                System.out.println("seqNo --" + seqNo);
                SimpleDateFormat formatter1 = new SimpleDateFormat("ddMMyyyy");

                //String recordId = finalMap.entrySet().stream().findFirst().get().getValue();
                String finalFolderName = "IN2260_01_" + formatter1.format(new Date()) + "_V1.2_IRA005471_U" + seqNo;
                for (HashMap<String, Object> cifConData : cifData) {
                    System.out.println("INSIDE EXECUTE fetchSequence size" + cifConData);

                    HashMap<String, Object> zipFileData = getZipFileData(con, String.valueOf(cifConData.get("CIFID")), String.valueOf(cifConData.get("PINSTID")));
                    HashMap<String, Object> zipBodyData = getZipBodyData(con, String.valueOf(cifConData.get("CIFID")), String.valueOf(cifConData.get("PINSTID")));
                    if (zipFileData.size() > 0 && zipBodyData.size() > 0) {

                        if (zipFileData.get("ID_PROOF_NUMBER").toString().length() == 12) {

                            if (!folderCreationStatus) {

                                File finalFold = new File(System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "CKYC_FINAL_MF" + File.separator + finalFolderName);
                                System.out.println("FOLDER -- " + finalFold.getAbsolutePath());
                                if (finalFold.exists()) {
                                    finalFold.delete();
                                    finalFold.mkdir();
                                } else {
                                    finalFold.mkdir();
                                }
                                folderCreationStatus = true;
                            }
                            totalSize++;
                            String maskedAadhaar = "XXXXXXXX" + zipFileData.get("ID_PROOF_NUMBER").toString().substring(8);

                            try {
                                Files.move(Paths.get(System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "MF" + File.separator + "PRE_CIF_ZIP" + File.separator + String.valueOf(cifConData.get("CIFID")) + ".zip"), Paths.get(System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL" + File.separator + "CKYC_FINAL_MF" + File.separator + finalFolderName + File.separator + String.valueOf(cifConData.get("CIFID")) + ".zip"), StandardCopyOption.REPLACE_EXISTING);
                                finalDataForBody += generate20(String.valueOf(totalSize), String.valueOf(cifConData.get("CIFID")), String.valueOf(zipBodyData.get("SALUTATION")), String.valueOf(zipBodyData.get("FIRSTNAME")), String.valueOf(zipBodyData.get("MIDDLENAME")), String.valueOf(zipBodyData.get("LASTNAME")),
                                        String.valueOf(zipBodyData.get("PANFIRSTNAME")), String.valueOf(zipBodyData.get("PANMIDDLENAME")), String.valueOf(zipBodyData.get("PANLASTNAME")), String.valueOf(zipBodyData.get("FATHERNAME")), String.valueOf(zipBodyData.get("MOTHERNAME")), String.valueOf(zipBodyData.get("GENDER")), dateConverter(String.valueOf(zipBodyData.get("DATEOFBIRTH"))), "IN", String.valueOf(zipBodyData.get("PANCARD")), String.valueOf(zipBodyData.get("FORM60")), String.valueOf(zipBodyData.get("HOUSENUMBERNAME")),
                                        String.valueOf(zipBodyData.get("LANDMARK")), String.valueOf(zipBodyData.get("LOCATION")), String.valueOf(zipBodyData.get("CITY")), String.valueOf(zipBodyData.get("DISTRICT")), String.valueOf(zipBodyData.get("STATE")), String.valueOf(zipBodyData.get("PINCODE")), String.valueOf(zipBodyData.get("IS_CORR_ADD_SAME")),
                                        String.valueOf(zipBodyData.get("POSTOFFICE")), String.valueOf(zipBodyData.get("MOBILE")), String.valueOf(zipBodyData.get("EMAIL")), dateConverter(String.valueOf(zipBodyData.get("ACCOUNTOPENDATE"))), String.valueOf(zipBodyData.get("INITIATEDBYNAME")), String.valueOf(zipBodyData.get("BRANCHNAME")), String.valueOf(zipBodyData.get("INITIATEDBYID")), String.valueOf(zipBodyData.get("HOUSENUMBERNAME_CORRS")), String.valueOf(zipBodyData.get("LANDMARK_CORRS")), String.valueOf(zipBodyData.get("LOCATION_CORRS")), String.valueOf(zipBodyData.get("CITY_CORRS")), String.valueOf(zipBodyData.get("DISTRICT_CORRS")), String.valueOf(zipBodyData.get("STATE_CORRS")), String.valueOf(zipBodyData.get("PINCODE_CORRS")));
                                finalDataForBody += "30|" + totalSize + "|E|" + maskedAadhaar + "|||||||||" + "\n";
                                finalDataForBody += "70|" + totalSize + "|" + zipFileData.get("PHOTOGRAPH_NAME").toString() + "|02||||||" + "\n";
                                finalDataForBody += "70|" + totalSize + "|" + zipFileData.get("ID_NAME").toString() + "|04||||||" + "\n";
                                updateFinalStatusUpdate(con, finalFolderName, String.valueOf(cifConData.get("CIFID")), String.valueOf(cifConData.get("PINSTID")));
                            } catch (Exception ex) {
                                totalSize--;
                                updateFinalStatusUpdate(con, "File not found", String.valueOf(cifConData.get("CIFID")), String.valueOf(cifConData.get("PINSTID")));
                                ex.printStackTrace();
                            }
                        } else {
                            /// Update in main table with Invalid aadhaar need to write code
                            updateFinalStatusUpdate(con, "ID proof mismatch", String.valueOf(cifConData.get("CIFID")), String.valueOf(cifConData.get("PINSTID")));
                        }
//
//                       
//                         | 
                        System.out.println("finalDataForBody - " + finalDataForBody);
                    } else {
                        System.out.println("INSIDE ELSE NO DATA FOUND EXECUTE cifData size");
                    }
                }
                String finalTextContent = "";
                if (!finalDataForBody.equals("")) {

                    SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
                    String finalHeader = "10|" + seqNo + "|IN2260|01|" + totalSize + "|" + formatter.format(new Date()) + "|V1.2|01||||" + "\n";
                    finalTextContent = finalHeader + finalDataForBody;
                    String fileName = System.getProperty("user.dir") + File.separator + "DMS_ARCHIVAL"
                            + File.separator + "CKYC_FINAL_MF" + File.separator + finalFolderName
                            + File.separator + finalFolderName + ".txt";

                    File txtFile = new File(fileName);
                    System.out.println("fileName Inside folder  : " + fileName);
                    FileWriter myWriter = new FileWriter(txtFile);
                    myWriter.write(finalTextContent);
                    myWriter.close();
                    System.out.println("Successfully wrote to the file.");
                }

                //folder creation
                //Header inside txt file
//            String finalHeader = "10|" + seqNo + "|IN2260|01|" + size + "|" + formatter.format(new Date()) + "|V1.2|01||||";
            } else {
                System.out.println("INSIDE ELSE NO DATA FOUND EXECUTE cifData size");
            }
        } catch (Exception ex) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            ex.printStackTrace();
        }

    }

    public String dateConverter(String date1) {
        String[] date = date1.split(" ")[0].split("-");
        String updatedDate = date[2] + "-" + date[1] + "-" + date[0];
        return updatedDate;

    }

    public String generate20(String rowNum, String CUSTOMER_ID, String SALUTATION, String F_NAME, String M_NAME, String L_NAME,
            String PAN_FNAME, String PAN_MNAME, String PAN_LNAME, String FATHER_NAME, String MOTHER_NAME, String GENDER, String CUST_DOB, String COUNTRY, String PANCARD, String Form60, String HNO_NAME,
            String LANDMARK, String LOCATION, String CITY, String SUB_DISTRICT, String STATE, String PINCODE, String PAN_AADHARSTATUS,
            String POST_OFFICE, String MOBILE, String EMAIL, String ACC_OPEN_DATE, String INIT_BY_NAME, String BRANCH_NAME, String INIT_BY_ID, String HOUSENUMBERNAME_CORRS, String LANDMARK_CORRS, String LOCATION_CORRS, String CITY_CORRS, String DISTRICT_CORRS, String STATE_CORRS, String PINCODE_CORRS) {
        String Proof = "FORM60";
        if (PANCARD != null) {
            Proof = PANCARD;
        }
        String text20 = "20|" + rowNum + "|01|50000|||||||||||01|||01|" + CUSTOMER_ID + "|" + SALUTATION + "|" + F_NAME + "|" + M_NAME + "|" + L_NAME + "||" + SALUTATION + "||" + PAN_FNAME + "|" + PAN_MNAME + "|" + PAN_LNAME + "|01|Mr|" + FATHER_NAME + "||||Mrs|" + MOTHER_NAME + "||||" + GENDER + "||||" + CUST_DOB + "|" + COUNTRY + "|||||||" + Proof + "||||||||" + HNO_NAME + " " + LANDMARK + "|" + LOCATION + "|" + LOCATION + " " + LANDMARK + "|" + CITY + "|" + SUB_DISTRICT + "|" + STATE + "|" + COUNTRY + "|" + PINCODE + "|01||" + PAN_AADHARSTATUS + "||" + HOUSENUMBERNAME_CORRS + " " + LANDMARK_CORRS + "|" + LOCATION_CORRS + " " + LANDMARK_CORRS + "|" + LANDMARK_CORRS + " " + POST_OFFICE + "|" + CITY_CORRS + "|" + DISTRICT_CORRS + "|" + STATE_CORRS + "|" + COUNTRY + "|" + PINCODE_CORRS + "||||||||||||||||91|" + MOBILE + "|||" + EMAIL + "||" + ACC_OPEN_DATE + "|" + SUB_DISTRICT + "|" + ACC_OPEN_DATE + "|01|" + INIT_BY_NAME + "|AUTHORIZED PERSON|" + BRANCH_NAME + "|" + INIT_BY_ID + "|ESAF SFB|IN2260|01|0|||02||||||" + "\n";
        return text20;
    }

    public HashMap<String, Object> getZipBodyData(Connection con, String cif, String Pinstid) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        HashMap<String, Object> zipCIFData = new HashMap<>();
        try {
            query = "select * from MF_CKYC_EXTRACT_ZIP_V2 where STATUS is null and CUSTOMERID = '" + cif + "' and PINSTID ='" + Pinstid + "'";
            pstmt = con.prepareCall(query);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                HashMap<String, Object> data = new HashMap<>();
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    if (rs.getString(rsmd.getColumnName(i)) != null) {
                        data.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
                    } else {
                        data.put(rsmd.getColumnName(i), "");
                    }
                }

                zipCIFData = data;
            }

        } catch (Exception ex) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            ex.printStackTrace();
        }
        return zipCIFData;

    }

    public HashMap<String, Object> getZipFileData(Connection con, String cif, String Pinstid) {
        System.out.println("INSIDE getZipFileData");
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        HashMap<String, Object> zipCIFData = new HashMap<>();
        try {
            query = "select PINSTID,CIFID,PHOTOGRAPH_NAME,ID_NAME,ID_PROOF_NUMBER,ID_PROOF_TYPE,FOLDER_PATH from MF_CIF_ZIP where PROCESSED is null and CIFID = '" + cif + "' and PINSTID ='" + Pinstid + "'";
            pstmt = con.prepareCall(query);
            rs = pstmt.executeQuery();
            System.out.println("query --" + query);
            while (rs.next()) {
                HashMap<String, Object> data = new HashMap<>();
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                boolean status = true;
                String colName = "";
                for (int i = 1; i <= columnCount; i++) {
                    System.out.println("INSIDE FOR LOOP ZIP Durai --" + status);
                    if (rs.getString(rsmd.getColumnName(i)).isEmpty()) {
                        System.out.println("INSIDE IF LOOP ZIP Durai --" + status);
                        colName = rsmd.getColumnName(i);
                        status = false;
                        break;
                    } else {
                        System.out.println("INSIDE ELSE LOOP ZIP Durai --" + status);
                        data.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
                    }
                }
                System.out.println("STATUS ZIP Durai --" + status);
                if (!status) {
                    updateFILERemarks(con, colName, rs.getString("CIFID"), rs.getString("PINSTID"));
                    zipCIFData = new HashMap<>();
                } else {
//                     updateFILEProcessedStatus(con, rs.getString("CIFID"), rs.getString("PINSTID"));
                    zipCIFData = data;
                }

            }

        } catch (Exception ex) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            ex.printStackTrace();
        }
        return zipCIFData;

    }

    public void updateFinalStatusUpdate(Connection con, String refNo, String CIF, String PINSTID) {

        try {
            String query = "";
            PreparedStatement pstmt = null;
            query = "UPDATE MF_CIF_ZIP_PRE SET CKYC_REF_NO ='" + refNo + "' WHERE CIFID = '" + CIF + "' and PINSTID = '" + PINSTID + "'";
            pstmt = con.prepareStatement(query);
            pstmt.executeUpdate();
        } catch (Exception ex) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            ex.printStackTrace();
        }

    }

    public void updateFILEProcessedStatus(Connection con, String CIF, String PINSTID) {

        try {
            String query = "";
            PreparedStatement pstmt = null;
            query = "UPDATE MF_CIF_ZIP SET PROCESSED='Y' WHERE CIFID = '" + CIF + "' and PINSTID = '" + PINSTID + "'";
            pstmt = con.prepareStatement(query);
            pstmt.executeUpdate();
        } catch (Exception ex) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            ex.printStackTrace();
        }

    }

    public void updateFILERemarks(Connection con, String columnName, String CIF, String PINSTID) {

        try {
            String query = "";
            PreparedStatement pstmt = null;
            query = "UPDATE MF_CIF_ZIP SET REMARKS = '" + columnName + " is empty" + "',PROCESSED='Y' WHERE CIFID = '" + CIF + "' and PINSTID = '" + PINSTID + "'";
            pstmt = con.prepareStatement(query);
            pstmt.executeUpdate();
        } catch (Exception ex) {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            ex.printStackTrace();
        }

    }

//    public List<HashMap<String, Object>> getZipCIFData(Connection con) {
//        System.out.println("INSIDE getZipCIFData ---");
//        String query = "";
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
//        List<HashMap<String, Object>> zipCIFData = new ArrayList<>();
//        try {
//            query = "select * from MF_CIF_ZIP_PRE where PROCESSED = 'Y' and DATA_PROCESSED = 'Y' and CKYC_REF_NO is null and rownum < 5000";
//            pstmt = con.prepareCall(query);
//            System.out.println("INSIDE MF_CIF_ZIP_PRE QUERY ---" + query);
//            rs = pstmt.executeQuery();
//            while (rs.next()) {
//                HashMap<String, Object> data = new HashMap<>();
//                ResultSetMetaData rsmd = rs.getMetaData();
//                int columnCount = rsmd.getColumnCount();
//                for (int i = 1; i <= columnCount; i++) {
//                    data.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
//                }
//                zipCIFData.add(data);
//
//                System.out.println(data);
//
//            }
//
//        } catch (Exception ex) {
//            if (con != null) {
//                try {
//                    con.close();
//                } catch (Exception exp) {
//                    exp.printStackTrace();
//                }
//            }
//            ex.printStackTrace();
//        }
//        return zipCIFData;
//
//    }
    public List<HashMap<String, Object>> getZipCIFData(Connection con) {
        System.out.println("INSIDE getZipCIFData ---");
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<HashMap<String, Object>> zipCIFData = new ArrayList<>();

        CallableStatement callableStatement = null;
        String getzipstatus = "{call MF_CIF_ZIP_CREATOR(?)}";

        try {

            callableStatement = con.prepareCall(getzipstatus);
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.executeUpdate();
            String picStatus = callableStatement.getString(1);
            
            System.err.println("Cheking the count in MF_CIF_ZIP_PRE is greator than 5000");
            switch (picStatus) {
                case "Y": {
                    System.err.println("MF_CIF_ZIP_PRE is greator than 5000 proceeding for zip");
                    try {
                        query = "select * from MF_CIF_ZIP_PRE where PROCESSED = 'Y' and DATA_PROCESSED = 'Y' and CKYC_REF_NO is null and rownum < 5000";
                        pstmt = con.prepareCall(query);
                        System.out.println("INSIDE MF_CIF_ZIP_PRE QUERY ---" + query);
                        rs = pstmt.executeQuery();
                        while (rs.next()) {
                            HashMap<String, Object> data = new HashMap<>();
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int columnCount = rsmd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                data.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
                            }
                            zipCIFData.add(data);

                            System.out.println(data);

                        }

                    } catch (Exception ex) {
                        if (con != null) {
                            try {
                                con.close();
                            } catch (Exception exp) {
                                exp.printStackTrace();
                            }
                        }
                        ex.printStackTrace();
                    }
                    break;
                }
                case "N": {
                    System.err.println("MF_CIF_ZIP_PRE is less than 5000 Skipping the zip");
                    break;
                }
            }

        } catch (SQLException e) {
            e.getCause();
            e.printStackTrace();
        }

        return zipCIFData;

    }

}
