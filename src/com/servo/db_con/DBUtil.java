/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.servo.db_con;

/**
 *
 * @author VINITH
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

/**
 *
 * @author durai
 */
public class DBUtil {

    public static Connection getDatabaseConnection() {
        try {
            Context initContext = new InitialContext();
            DataSource dataSource = (DataSource) initContext.lookup("java:jboss/jdbc/oracleJNDI");
            //DataSource dataSource = (DataSource) initContext.lookup("java:jboss/jdbc/oracleJNDIReport");
            System.out.println(":::::::::::::::::::::::::::::::: Got connection :::::::::::::::::::::::");
            return dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
        
    public static Connection getCASAConnection() {
        Connection conn = null;
        Properties prop;
        try {
            System.out.println("[CLP] - INSIDE CASA DBCONNECTION CLASS --");
            InputStream inputStream = new FileInputStream(System.getProperty("user.dir") + File.separator
                    + "SRVConfig" + File.separator + "DatabaseDetails.properties");
            prop = new Properties();
            prop.load(inputStream);
            Class.forName(prop.getProperty("driver"));
            conn = DriverManager.getConnection(
                    prop.getProperty("url"), prop.getProperty("casausername"), prop.getProperty("casapassword"));

        } catch (ClassNotFoundException ex) {
            System.out.println("class not found : " + ex.getMessage());
            ex.printStackTrace();
        } catch (SQLException ex) {
            System.out.println("database exception : " + ex.getMessage());
            ex.printStackTrace();
        } catch (FileNotFoundException ex) {
            System.out.println("File not found exception : " + ex.getMessage());
            ex.printStackTrace();
        } catch (Exception ex) {
            System.out.println("exception : " + ex.getMessage());
            ex.printStackTrace();
        }
        return conn;
    }

    public static void closeConnection(Connection con, PreparedStatement ps, ResultSet rs) {
        try {
            if (con != null) {
                con.close();
                con = null;
            }
            if (ps != null) {
                ps.close();
                ps = null;
            }
            if (rs != null) {
                rs.close();
                rs = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                    con = null;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            if (ps != null) {
                try {
                    ps.close();
                    ps = null;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        }
    }

    public static void closeStatement(PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
                ps = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                    ps = null;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void closeResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
