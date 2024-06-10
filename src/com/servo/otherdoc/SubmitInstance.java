package com.servo.otherdoc;

import com.servo.businessdoc.SessionService;
import com.servo.configdoc.propertyConfig;
import com.servo.error.InvalidStatusException;
import com.servo.error.SRVStatus;
import com.servo.output.RoutingResponse;
import com.servo.service.routing.SRVRouting;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SubmitInstance {

    SRVRouting routing;

    public RoutingResponse submitPInstance(Connection con, String pinstId, propertyConfig config) {

        int lockStatus;
        Integer userId;
        Integer activityId;
        RoutingResponse routingResponse = new RoutingResponse();
        try {
            routing = new SRVRouting();
            checkSession(con, config.getUserId(), config);
            System.out.println("[SRVSubmitInstanceManager]--submitPInstance()-Inside submit call going to Lock.");
            lockStatus = lockPInastance(con, pinstId, config.getUserId(),config.getActivityId());

            if (lockStatus == 0) {
                try {
                    userId = config.getUserId();
                    activityId = config.getActivityId();
                    System.out.println(userId + ",," + activityId);
                    routingResponse = routing.SRVSubmitProcessInstanceEx(con, userId, pinstId, 1, activityId);
                } catch (InvalidStatusException ex) {
                    routingResponse.setStatus(ex.getStatus());
                    routingResponse.setInfo(ex.getMessage());
                    System.out.println("[SRVSubmitInstanceManager]--submitPInstance()--Routing Failed...." + ex);
//                    unlockPInstance(con, pinstId, activityId);
                    return routingResponse;
                }

                if (routingResponse.getStatus() == SRVStatus.SUCCESS) {
                    lockStatus = unlockPInstance(con, pinstId, activityId);
                } else {
                    lockStatus = unlockPInstance(con, pinstId, activityId);
                }

            } else {
                System.out.println("CAnt lock");
            }

        } catch (Exception ex) {
            routingResponse.setStatus(1);
            routingResponse.setInfo(ex.getCause().toString());
        }
        return routingResponse;
    }

    public void checkSession(Connection con, int userId, propertyConfig config) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String query = "";
        int count = 0;
        SessionService service = new SessionService();
        try {
            query = "SELECT COUNT(SESSIONID) AS SESSIONCOUNT FROM SRV_RU_CONNECTIONINFO WHERE USERID = ?";
            pstmt = con.prepareStatement(query);
            pstmt.setInt(1, userId);

            rs = pstmt.executeQuery();
            rs.next();

            count = rs.getInt("SESSIONCOUNT");
            closeConnection(rs, pstmt);
            if (count == 0) {
                service.getSession(config.getIstreamsIp(), config.getIstreamsPort(), config.getUserName(), "system123#");
            }
        } catch (Exception ex) {

        } finally {
            closeConnection(rs, pstmt);
        }
    }

    public int lockPInastance(Connection con, String pinstId, int userId, int activityId) {
        PreparedStatement pstmt = null;
        int res = 1;
        try {
            String sql = "UPDATE SRV_RU_EXECUTION SET LOCKEDBYID =?, LOCKEDBYNAME=?  , "
                    + "LOCKEDTIME=SYSDATE , EXECUTIONFLAG=? "
                    + "where PROCESSINSTANCEID=? and FORKID=? and ACTIVITYID=? ";
            pstmt = con.prepareStatement(sql);
            pstmt.setInt(1, userId);
            pstmt.setString(2, userId + "");
            pstmt.setString(3, "D");
            pstmt.setString(4, pinstId);
            pstmt.setInt(5, 1);
            pstmt.setInt(6, activityId);
            res = pstmt.executeUpdate();
            if (res != 1) {

            } else {
                res = 0;
            }
            closeConnection(null, pstmt);
        } catch (SQLException ex) {
            System.out.println("Error in lock pInstance-->" + ex);
        } finally {
            closeConnection(null, pstmt);
            return res;
        }
    }

    public int unlockPInstance(Connection con, String pinstId, int activityId) {
        PreparedStatement pstmt = null;
        int res = 1;
        try {
            String sql = "UPDATE SRV_RU_EXECUTION SET LOCKEDBYID = null, LOCKEDBYNAME = null , "
                    + "LOCKEDTIME=null "
                    + "where PROCESSINSTANCEID=? and FORKID=? and ACTIVITYID = ?";
            pstmt = con.prepareStatement(sql);
            pstmt.setString(1, pinstId);
            pstmt.setInt(2, 1);
            pstmt.setInt(3, activityId);

            res = pstmt.executeUpdate();
            if (res != 1) {

            } else {
                res = 0;
            }
            closeConnection(null, pstmt);
        } catch (SQLException ex) {
            System.out.println("Error in unLock pInstance-->" + ex);
        } finally {
            closeConnection(null, pstmt);
        }
        return res;
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
