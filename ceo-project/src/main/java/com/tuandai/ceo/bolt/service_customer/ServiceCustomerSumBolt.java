package com.tuandai.ceo.bolt.service_customer;


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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceCustomerSumBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(ServiceCustomerSumBolt.class);
    ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();

    @Override
    public void prepare(Map map, TopologyContext context) {
        List<Result> list = HBaseUtils.getRowsByColumns("service_custorm_profession_spread", "", "cf", new String[]{"profession"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("profession".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.SERVICE_CUSTOMER_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String profession=rs.getString("profession");
                    String cnt=rs.getString("cnt");
                    HBaseUtils.insert("service_custorm_profession_spread",profession,"cf","profession",cnt);
                    countMap.put(profession,Integer.valueOf(cnt));
                }
            }catch (Exception e){
                logger.error("SumAmountResultBolt sql execute fail",e);
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
        int num=0;
        String customer_name = tuple.getStringByField("customer_name");
        String profession = tuple.getStringByField("profession");

        if (countMap.get(profession) == null) {
            countMap.put(profession, 1);
        } else {
            num += countMap.get(profession)+1;
            countMap.put(profession, num);
        }

        collector.emit(new Values(profession, countMap.get(profession)));
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("profession", "num"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
