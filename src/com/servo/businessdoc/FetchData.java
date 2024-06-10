package com.servo.businessdoc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class FetchData {
    
    
       public HashMap fetchAmlData(Connection con, String pinstId) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        HashMap dataMap = new HashMap();
        String proffType = "";
        String secondproffType = "";
        try {
            query = "select SATELITE_OFFICE_ID,CUSTOMER_ID,FIRSTNAME,MIDDLENAME,LASTNAME,dob AS DOB,IDPROOFNUMBER,STREET_AREA,CITY_VILLAGE_TOWN,POST_OFFICE,DISTRICT,"
                    + "STATE,PINCODE,IDPROOFTYPE,ASSOCIATIONID,SECOND_IDTYPE,SECOND_IDNUMBER,EXISTING_CUST,ISCIF_LOAN,CIFID,SALUTATION,case  when SALUTATION='1' then 'M' else 'F' end as GENDER,AADHAR_REFRENCE_NUMMBER from FIG_EXT where pinstid =?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, pinstId);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                dataMap.put("Bank_Branch_ID", rs.getString("SATELITE_OFFICE_ID"));
                dataMap.put("Customer_Id", (rs.getString("CUSTOMER_ID") == null) ? "" : rs.getString("CUSTOMER_ID"));
                dataMap.put("First_Name", (rs.getString("FIRSTNAME") == null) ? "" : rs.getString("FIRSTNAME"));
                dataMap.put("Middle_Name", (rs.getString("MIDDLENAME") == null) ? "" : rs.getString("MIDDLENAME"));
                dataMap.put("Last_Name", (rs.getString("LASTNAME") == null) ? "" : rs.getString("LASTNAME"));
                dataMap.put("Date_of_Birth", (rs.getString("DOB") == null) ? "" : rs.getString("DOB"));
                dataMap.put("Identification_Type", (rs.getString("IDPROOFTYPE") == null) ? "" : rs.getString("IDPROOFTYPE"));
                dataMap.put("Address1", (rs.getString("STREET_AREA") == null) ? "" : rs.getString("STREET_AREA"));
                dataMap.put("Address2", (rs.getString("CITY_VILLAGE_TOWN") == null) ? "" : rs.getString("CITY_VILLAGE_TOWN"));
                dataMap.put("Address3", (rs.getString("POST_OFFICE") == null) ? "" : rs.getString("POST_OFFICE"));
                dataMap.put("City", (rs.getString("DISTRICT") == null) ? "" : rs.getString("DISTRICT"));
                dataMap.put("State", (rs.getString("STATE") == null) ? "" : rs.getString("STATE"));
                dataMap.put("Country", "IN");
                dataMap.put("Gender", (rs.getString("GENDER") == null) ? "" : rs.getString("GENDER"));
                dataMap.put("Zipcode", (rs.getString("PINCODE") == null) ? "" : rs.getString("PINCODE"));
                dataMap.put("Identification_Number", (rs.getString("IDPROOFNUMBER") == null) ? "" : rs.getString("IDPROOFNUMBER"));
                dataMap.put("ASSOCIATIONID", (rs.getString("ASSOCIATIONID") == null) ? "" : rs.getString("ASSOCIATIONID"));
                dataMap.put("SECOND_IDTYPE", (rs.getString("SECOND_IDTYPE") == null) ? "" : rs.getString("SECOND_IDTYPE"));
                dataMap.put("SECOND_IDNUMBER", (rs.getString("SECOND_IDNUMBER") == null) ? "" : rs.getString("SECOND_IDNUMBER"));
                dataMap.put("EXISTING_CUST", (rs.getString("EXISTING_CUST") == null) ? "" : rs.getString("EXISTING_CUST"));
                dataMap.put("ISCIF_LOAN", (rs.getString("ISCIF_LOAN") == null) ? "N" : rs.getString("ISCIF_LOAN"));
                dataMap.put("CIFID", (rs.getString("CIFID") == null) ? "N" : rs.getString("CIFID"));
                dataMap.put("AADHAR_REFRENCE_NUMMBER", (rs.getString("AADHAR_REFRENCE_NUMMBER") == null) ? "" : rs.getString("AADHAR_REFRENCE_NUMMBER"));
            }
            closeConnection(rs, pstmt);

            proffType = dataMap.get("Identification_Type").toString();
            if (proffType.toUpperCase().equals("1")) {
                dataMap.put("AADHAR", dataMap.get("Identification_Number").toString());
            } else {
                dataMap.put("AADHAR", "");
            }
            if (proffType.toUpperCase().equals("2")) {
                dataMap.put("VOTERID", dataMap.get("Identification_Number").toString());
            } else {
                dataMap.put("VOTERID", "");
            }
            if (proffType.toUpperCase().equals("3")) {
                dataMap.put("PAN", dataMap.get("Identification_Number").toString());
            } else {
                dataMap.put("OTHER", "");
            }
            
            
            
            secondproffType = dataMap.get("SECOND_IDTYPE").toString();
             if (secondproffType.toUpperCase().equals("2") )  {
                dataMap.put("VOTERID", (dataMap.get("Identification_Number") == null) ? "" : dataMap.get("SECOND_IDNUMBER"));
            }
            
             
            if (secondproffType.toUpperCase().equals("4")) {
                dataMap.put("DRIVING", (dataMap.get("SECOND_IDNUMBER") == null) ? "" : dataMap.get("SECOND_IDNUMBER"));
            } 

            if (secondproffType.toUpperCase().equals("6")) {
                dataMap.put("PASSPORT", (dataMap.get("SECOND_IDNUMBER") == null) ? "" : dataMap.get("SECOND_IDNUMBER"));
            }
            
            
            
            System.out.println(dataMap);
        } catch (Exception ex) {
            System.out.println("EXception in Fetching Data for Aml - " + ex);
            ex.printStackTrace();
            return dataMap;
        } finally {
            closeConnection(rs, pstmt);
        }
        return dataMap;
    }
 
    

//    public HashMap fetchAmlData(Connection con, String pinstId) {
//        String query = "";
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
//        HashMap dataMap = new HashMap();
//        String proffType = "";
//        try {
//            query = "select SATELITE_OFFICE_ID,CUSTOMER_ID,FIRSTNAME,MIDDLENAME,LASTNAME,DOB,IDPROOFNUMBER,STREET_AREA,CITY_VILLAGE_TOWN,POST_OFFICE,DISTRICT,"
//                    + "STATE,PINCODE,IDPROOFTYPE,ASSOCIATIONID,SECOND_IDTYPE,SECOND_IDNUMBER,EXISTING_CUST,ISCIF_LOAN,CIFID from MF_EXT where pinstid =?";
//            pstmt = con.prepareStatement(query);
//            pstmt.setString(1, pinstId);
//            rs = pstmt.executeQuery();
//
//            while (rs.next()) {
//                dataMap.put("Bank_Branch_ID", rs.getString("SATELITE_OFFICE_ID"));
//                dataMap.put("Customer_Id", (rs.getString("CUSTOMER_ID") == null) ? "" : rs.getString("CUSTOMER_ID"));
//                dataMap.put("First_Name", (rs.getString("FIRSTNAME") == null) ? "" : rs.getString("FIRSTNAME"));
//                dataMap.put("Middle_Name", (rs.getString("MIDDLENAME") == null) ? "" : rs.getString("MIDDLENAME"));
//                dataMap.put("Last_Name", (rs.getString("LASTNAME") == null) ? "" : rs.getString("LASTNAME"));
//                dataMap.put("Date_of_Birth", (rs.getString("DOB") == null) ? "" : rs.getString("DOB"));
//                dataMap.put("Identification_Type", (rs.getString("IDPROOFTYPE") == null) ? "" : rs.getString("IDPROOFTYPE"));
//                dataMap.put("Address1", (rs.getString("STREET_AREA") == null) ? "" : rs.getString("STREET_AREA"));
//                dataMap.put("Address2", (rs.getString("CITY_VILLAGE_TOWN") == null) ? "" : rs.getString("CITY_VILLAGE_TOWN"));
//                dataMap.put("Address3", (rs.getString("POST_OFFICE") == null) ? "" : rs.getString("POST_OFFICE"));
//                dataMap.put("City", (rs.getString("DISTRICT") == null) ? "" : rs.getString("DISTRICT"));
//                dataMap.put("State", (rs.getString("STATE") == null) ? "" : rs.getString("STATE"));
//                dataMap.put("Country", "IND");
//                dataMap.put("Zipcode", (rs.getString("PINCODE") == null) ? "" : rs.getString("PINCODE"));
//                dataMap.put("Identification_Number", (rs.getString("IDPROOFNUMBER") == null) ? "" : rs.getString("IDPROOFNUMBER"));
//                dataMap.put("ASSOCIATIONID", (rs.getString("ASSOCIATIONID") == null) ? "" : rs.getString("ASSOCIATIONID"));
//                dataMap.put("SECOND_IDTYPE", (rs.getString("SECOND_IDTYPE") == null) ? "" : rs.getString("SECOND_IDTYPE"));
//                dataMap.put("SECOND_IDNUMBER", (rs.getString("SECOND_IDNUMBER") == null) ? "" : rs.getString("SECOND_IDNUMBER"));
//                dataMap.put("EXISTING_CUST", (rs.getString("EXISTING_CUST") == null) ? "" : rs.getString("EXISTING_CUST"));
//                dataMap.put("ISCIF_LOAN", (rs.getString("ISCIF_LOAN") == null) ? "N" : rs.getString("ISCIF_LOAN"));
//                dataMap.put("CIFID", (rs.getString("CIFID") == null) ? "N" : rs.getString("CIFID"));
//
//            }
//            closeConnection(rs, pstmt);
//
//            proffType = dataMap.get("Identification_Type").toString();
//            if (proffType.toUpperCase().equals("1")) {
//                dataMap.put("AADHAR", dataMap.get("Identification_Number").toString());
//            } else {
//                dataMap.put("AADHAR", "");
//            }
//            if (proffType.toUpperCase().equals("2")) {
//                dataMap.put("VOTERID", dataMap.get("Identification_Number").toString());
//            } else {
//                dataMap.put("VOTERID", "");
//            }
//            if (proffType.toUpperCase().equals("3")) {
//                dataMap.put("PAN", dataMap.get("Identification_Number").toString());
//            } else {
//                dataMap.put("OTHER", "");
//            }
//            System.out.println(dataMap);
//        } catch (Exception ex) {
//            System.out.println("EXception in Fetching Data for Aml - " + ex);
//            ex.printStackTrace();
//            return dataMap;
//        } finally {
//            closeConnection(rs, pstmt);
//        }
//        return dataMap;
//    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String pinstid = null;
        Class.forName("oracle.jdbc.driver.OracleDriver");
        con = DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.9:1521:orcl1", "istreams123", "istreams123#");
        System.out.println("Connection Created Successfully.....");
        FetchData data = new FetchData();
        data.fetchAmlData(con, "MF-00000000625-PRO");
        con.close();
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
