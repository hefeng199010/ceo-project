package com.tuandai.ceo.bolt.sum_order;

import com.tuandai.ceo.enums.PrepareSql;
import com.tuandai.tools.HBaseUtils;
import com.tuandai.tools.JdbcProperties;
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

public class SumOrderResultBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(SumOrderResultBolt.class);

    Map<String, Long> countMap = new HashMap<String, Long>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        List<Result> list = HBaseUtils.getRowsByColumns("total_out_put_orders", "sum_order", "cf", new String[]{"sum_order"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("sum_order".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if (countMap.isEmpty()) {
            //hbase 为空，从mysql导入
            String sql = PrepareSql.SUM_ORDER_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try {
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String total_sum_amt=rs.getString("total_sum_amt");
                    HBaseUtils.insert("total_out_put_orders", "sum_order", "cf", "sum_order",  total_sum_amt);
                    countMap.put("sum_order",Long.valueOf(total_sum_amt));
                }
            } catch (Exception e) {
                logger.error("SumOrderResultBolt sql execute fail", e);
            }finally {
                try {
                    JdbcProperties.CutConnection(conn,rs);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Long sum_order = 0L;

        String business_id = tuple.getStringByField("business_id");

        if (countMap.get("sum_order") == null) {
            countMap.put("sum_order", 1L);
        } else {
            sum_order = countMap.get("sum_order") + 1;
            countMap.put("sum_order", sum_order);
            for (String key : countMap.keySet()) {
                HBaseUtils.insert("total_out_put_orders", key, "cf", "sum_order", countMap.get(key) + "");
            }
        }
//        if(!quchong.containsKey(business_id)){
//            quchong.put(business_id,business_id);
//            sum_order= countMap.get("sum_order")+1;
//            countMap.put("sum_order",sum_order);
//            for (String key : countMap.keySet()) {
//                HBaseUtils.insert("total_out_put_orders", key, "cf", "sum_order", countMap.get(key) + "");
//            }
//        }

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
