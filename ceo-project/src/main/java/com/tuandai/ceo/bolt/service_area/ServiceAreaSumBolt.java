package com.tuandai.ceo.bolt.service_area;


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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceAreaSumBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(ServiceAreaSumBolt.class);
    ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();
    Map<String,String>  quchong=new HashMap<>();
    @Override
    public void prepare(Map map, TopologyContext context) {
        List<Result> list = HBaseUtils.getRowsByColumns("service_custom_area_spread", "", "cf", new String[]{"area"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("area".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.SERVICE_AREA_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String province=rs.getString("province");
                    String cnt=rs.getString("cnt");
                    HBaseUtils.insert("service_custom_area_spread",province,"cf","area",cnt);
                    countMap.put(province,Integer.valueOf(cnt));
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
        Integer sum = 0;
        String id_card = tuple.getStringByField("id_card");
        String area = tuple.getStringByField("area");


        if(!quchong.containsKey(id_card)){
            quchong.put(id_card,id_card);
            sum= countMap.get(area)+1;
            countMap.put(area,sum);
            for (String key : countMap.keySet()) {
                HBaseUtils.insert("service_custom_area_spread", key, "cf", "area", countMap.get(key) + "");
            }
        }
        collector.emit(new Values(area, countMap.get(area)));
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area", "num"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
