package com.tuandai.ceo.bolt.business_balance;

import com.tuandai.ceo.enums.PrepareSql;
import com.tuandai.ceo.utils.CommonDoubleMap;
import com.tuandai.tools.HBaseUtils;
import com.tuandai.tools.JdbcProperties;
import com.tuandai.tools.JdbcProperties2;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BusinessBalanceResultBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(BusinessBalanceResultBolt.class);

    Map<String, Double> countMap = new HashMap<String, Double>();
    java.text.NumberFormat nf=null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        List<Result> list = HBaseUtils.getRowsByColumns("total_business_balance","balance_amount","cf",new String[]{"balance_amount"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("balance_amount".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.BUSINESS_BALANCE_SQL1.getSql();
            String sql2 = PrepareSql.BUSINESS_BALANCE_SQL2.getSql();
            ResultSet rs = null;
            Connection conn=null;
            ResultSet rs2 = null;
            Connection conn2=null;
            double factAmount=0L;
            double actual_total=0L;
            try{
                conn = JdbcProperties2.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                     factAmount=Double.parseDouble(rs.getString("factAmount"));
                }
                conn2 = JdbcProperties.ConnectMysql();
                Statement stmt2 =conn2.createStatement();
                rs2 = stmt2.executeQuery(sql2);
                while (rs2.next()){
                     actual_total=Double.parseDouble(rs2.getString("actual_total"));
                }
                HBaseUtils.insert("total_business_balance","balance_amount","cf","balance_amount",nf.format(factAmount+actual_total));
                countMap.put("balance_amount",factAmount+actual_total);

            }catch (Exception e){
                logger.error("BusinessBalanceResultBolt sql execute fail",e);
            }finally {
                try {
                    JdbcProperties.CutConnection(conn,rs);
                    JdbcProperties.CutConnection(conn2,rs2);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        double sum=0;
        try {
            double balance_amount = Double.parseDouble(tuple.getStringByField("amount"));
            // CommonDoubleMap.sumValue(countMap,"balance_amount",balance_amount);
            if(countMap.get("balance_amount")==null){
                sum+=balance_amount;
                countMap.put("balance_amount",sum);
            }
            else{
                sum+=countMap.get("balance_amount")+balance_amount;
                countMap.put("balance_amount",sum);
            }
            for (String key : countMap.keySet()) {
                HBaseUtils.insert("total_business_balance", key, "cf", "balance_amount", nf.format(countMap.get(key)));
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
