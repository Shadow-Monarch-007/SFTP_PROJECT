package com.servo.businessdoc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class MiscCalls {

    public Map<String, Object> updateAssociationCount(Connection con, HashMap inputMap, String type) {
        Map<String, Object> outputMap = new HashMap<String, Object>();
        PreparedStatement pstmt = null;
        String query = "";
        int dedupeCount = 0;
        try {
            if (type.equalsIgnoreCase("DEDUPE")) {
                query = "Update FIG_CM_PROSCOUNT SET TOTAL_DEDUPE = TOTAL_DEDUPE+1 where ASSOCIATIONID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, inputMap.get("ASSOCIATIONID").toString());
                dedupeCount = pstmt.executeUpdate();
                closeCursors(null, pstmt);
            } else if (type.equalsIgnoreCase("AML")) {
                query = "Update FIG_CM_PROSCOUNT SET TOTAL_AML = TOTAL_AML+1 where ASSOCIATIONID = ?";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, inputMap.get("ASSOCIATIONID").toString());
                dedupeCount = pstmt.executeUpdate();
                closeCursors(null, pstmt);
            }

            outputMap.put("status", 0);
            outputMap.put("message", "success");

        } catch (Exception ex) {
            System.out.println("[MFService - updateAssociationCount]-->" + ex);
            outputMap.put("status", 1);
            outputMap.put("message", ex);
            return outputMap;
        } finally {
            closeCursors(null, pstmt);
        }
        return outputMap;
    }

    public Map deDuplicacy(Connection con, String idNo, String idType, String idN02, String idType2,String refNo, String pinstId) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;
        Boolean returnFlag = Boolean.FALSE;
        Map statMap = new HashMap();
        Map instanceMap = new HashMap();
        int instanceCount = 0;
        String duplicateInstance = "";
        try {
            query = "SELECT PINSTID FROM FIG_EXT WHERE  AADHAR_REFRENCE_NUMMBER = ? AND (LOANGENRATION_FLAG != 'DONE' OR LOANGENRATION_FLAG IS NULL)";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, refNo);

            rs = pstmt.executeQuery();
            while (rs.next()) {
                instanceMap.put(instanceCount, rs.getString("pinstid"));
                instanceCount++;
            }
            closeCursors(rs, pstmt);
            System.out.println("Count Found For Duplicacy - " + instanceCount);
            if (instanceCount > 1) {
                statMap.put("STAT", "F");
                for (int i = 0; i < instanceMap.size(); i++) {
                    if (!instanceMap.get(i).toString().equals(pinstId)) {
                        duplicateInstance = instanceMap.get(i).toString();
                    }
                }
                statMap.put("REASON", "Duplicasy Found For Primery Id in " + duplicateInstance);
            } else {
                instanceCount = 0;
                instanceMap.clear();
                query = "select pinstid from FIG_EXT where  SECOND_IDNUMBER = ? AND (LOANGENRATION_FLAG != 'DONE' OR LOANGENRATION_FLAG IS NULL)";
                pstmt = con.prepareStatement(query);
                pstmt.setString(1, idN02);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    instanceMap.put(instanceCount, rs.getString("pinstid"));
                    instanceCount++;
                }
                closeCursors(rs, pstmt);
                if (instanceCount > 1) {
                    statMap.put("STAT", "F");
                    for (int i = 0; i < instanceMap.size(); i++) {
                        if (!instanceMap.get(i).toString().equals(pinstId)) {
                            duplicateInstance = instanceMap.get(i).toString();
                        }
                    }
                    statMap.put("REASON", "Duplicasy Found For Secondry Id in " + duplicateInstance);
                } else {
                    statMap = deDuplicacy2(con, idNo, idType);
                }
            }
            return statMap;
        } catch (Exception ex) {
            System.out.println("Duplicasy Exe-" + ex);
            ex.printStackTrace();
            return statMap;
        } finally {
            closeCursors(null, pstmt);
        }
    }

    public Map deDuplicacy2(Connection con, String idNo, String idType) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;
        Boolean returnFlag = Boolean.FALSE;
        Map statMap = new HashMap();
        try {
            if (idType.equals("1")) {
                query = "select count(AADHARNUMBER) as dedupe from FIG_MS_DEDUPE where AADHARNUMBER = ?";
            } else if (idType.equals("2")) {
                query = "select count(VOTERID) as dedupe from FIG_MS_DEDUPE where VOTERID = ?";
            } else if (idType.equals("3")) {
                query = "select count(PAN) as dedupe from FIG_MS_DEDUPE where PAN = ?";
            } else {
                query = "select count(PASSPORTNUMBER) as dedupe from FIG_MS_DEDUPE where PASSPORTNUMBER = ?";
            }
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, idNo);

            rs = pstmt.executeQuery();
            rs.next();

            count = rs.getInt("dedupe");
            closeCursors(rs, pstmt);
            System.out.println("Count Found For Duplicacy2 - " + count);
            if (count > 0) {
                statMap.put("STAT", "F");
                statMap.put("REASON", "Duplicasy Found For Primery Id Proof " + idNo);
            } else {
                statMap.put("STAT", "T");
                statMap.put("REASON", "Duplicasy Not Found");
            }
        } catch (Exception ex) {
            statMap.put("STAT", "F");
            statMap.put("REASON", ex.getMessage());
            return statMap;
        } finally {
            closeCursors(rs, pstmt);
        }
        return statMap;
    }

    public static String fetchRelId(Connection con, String relId) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String relCode = "";
        try {
            query = "SELECT RELATIONSHIPCODE FROM FIG_MS_RELATIONSHIPTYPE WHERE RELATIONSHIPTYPEID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, relId);

            rs = pstmt.executeQuery();
            rs.next();
            relCode = rs.getString("RELATIONSHIPCODE");
            closeCursors(rs, pstmt);

        } catch (Exception ex) {
            return relCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return relCode;
    }

    public static String fetchMaritalId(Connection con, String relId) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String relCode = "";
        try {
            query = "SELECT MSTATUSCODE FROM FIG_MS_MARITALSTATUS WHERE MARITALSTATUSID = ?";

            pstmt = con.prepareStatement(query);
            pstmt.setString(1, relId);

            rs = pstmt.executeQuery();
            rs.next();
            relCode = rs.getString("MSTATUSCODE");
            closeCursors(rs, pstmt);

        } catch (Exception ex) {
            return relCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return relCode;
    }

    public static String fetchID(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT CODEPROOFTYPE FROM FIG_MS_IDPROOF WHERE IDPROOFID = ?";

            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("CODEPROOFTYPE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchAddressID(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT CODEPROOFTYPE FROM FIG_MS_ADDRESSPROOF WHERE PROOFID = ?";

            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("CODEPROOFTYPE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchCaste(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT CASTECODE FROM FIG_MS_CASTE WHERE CASTEID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("CASTECODE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchOccupation(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT OCCUPATIONCODE FROM FIG_MS_OCCUPATION WHERE OCCUPATIONID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("OCCUPATIONCODE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchPhyDisablity(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_ms_physicaldisability WHERE physicaldisabilityid = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchHouseType(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_MS_HOUSETYPE WHERE HOUSETYPEID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchDrinkingWater(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_ms_drinkingwater WHERE DRINKINWATERID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchLand(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_MS_TOTALLAND WHERE LANDID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchYesNo(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT OPTIONSUBCODE FROM MF_MS_OPTION WHERE OPTIONCODE = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("OPTIONSUBCODE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }
    
    public static String fetchYesNoTVVECHILE(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT OPTIONSUBCODE FROM MF_MS_OPTIONOTHERS WHERE OPTIONCODE = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("OPTIONSUBCODE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchHouseHold(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_MS_HOUSEHOLDMEMBER WHERE HOUSEHOLDMEMBERID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchVehicalType(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_MS_VEHICLE WHERE VEHICLEID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchEducation(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM mf_ms_educationlevel WHERE EDUCATIONLEVELID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id); 

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchResidence(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_MS_RESIDINGAREA WHERE RESIDINGAREAID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchAccType(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT SUBCODEID FROM FIG_MS_ACCOUNTTYPE WHERE ACCOUNTTYPEID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("SUBCODEID");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchRationCard(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT RATIONCARDTYPE FROM FIG_MS_RATIONCARDTYPE WHERE RATIONCARDID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("RATIONCARDTYPE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    public static String fetchReligion(Connection con, String id) {
        String query = "";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String idCode = "";
        try {
            query = "SELECT RELIGINCODE FROM FIG_MS_RELIGION WHERE RELIGIONID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setString(1, id);

            rs = pstmt.executeQuery();
            rs.next();
            idCode = rs.getString("RELIGINCODE");
            closeCursors(rs, pstmt);
        } catch (Exception ex) {
            return idCode;
        } finally {
            closeCursors(rs, pstmt);
        }
        return idCode;
    }

    private static void closeCursors(ResultSet rs, PreparedStatement ps) {
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
