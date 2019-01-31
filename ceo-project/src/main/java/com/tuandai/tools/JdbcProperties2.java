package com.tuandai.tools;

import com.tuandai.ceo.bolt.apply_output.ApplyOutputSumBolt;
import org.apache.log4j.Logger;

import java.sql.*;

public class JdbcProperties2 {
    protected static Logger logger = Logger.getLogger(ApplyOutputSumBolt.class);
//    static String user = "hongte_alms";
//    static String password = "EfjsfHM5";
//    static String url = "jdbc:mysql://172.16.200.111:3306/hongte_alms";
//    static String driver = "com.mysql.jdbc.Driver";

    static String user = "large_screen";
    static String password = "YNkjizQ6";
    static String url = "jdbc:mysql://10.100.1.86:3306/hongte_alms";
    static String driver = "com.mysql.jdbc.Driver";
    static Connection conn = null;
    static ResultSet rs = null;


    public static Connection ConnectMysql(){

        try{
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed()) {
                logger.info("mysql 初始化 success");
            } else {
                logger.error("mysql 初始化 fail");
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }

        return conn;
    }

    public static void CutConnection(Connection conn) throws SQLException{
        try{
            if(conn!=null){
                conn.close();
            }
        }catch(Exception e){
            logger.error("cut conn error",e);
        }
    }
    public static void CutConnection(Connection conn,ResultSet rs) throws SQLException{
        CutConnection(conn);
        if(rs!=null){
            rs.close();
        }
    }
    public static ResultSet getRs(String sql)throws SQLException{
        ResultSet rs = null;
        try{
            conn = ConnectMysql();
            Statement stmt =conn.createStatement();
            rs = stmt.executeQuery(sql);

        }catch (Exception e){
            logger.error("sql execute fail",e);
        }finally {
           // CutConnection(conn);
        }
        return rs;
    }

    public static void main(String[] args) throws SQLException {
        String sql = " SELECT SUM(\n" +
                "     CASE t2.plan_item_type\n" +
                "     WHEN 10\n" +
                "     THEN IFNULL(t2.fact_amount, 0)\n" +
                "     ELSE 0 END\n" +
                " ) factAmount FROM `hongte_alms`.tb_repayment_biz_plan_list_detail t2;";
        try{
            conn = ConnectMysql();
            Statement stmt =conn.createStatement();
            boolean ret = stmt.execute(sql);
            System.out.println(ret);

        }catch (Exception e){
            System.out.println("sql fail");
        }finally {
            if(conn!=null){
                conn.close();
            }
            if(rs!=null){
                rs.close();
            }
        }


    }
}
