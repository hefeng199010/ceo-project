package com.tuandai.ceo.bolt.sum_service_person;

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

public class SumServicePersonResultBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(SumServicePersonResultBolt.class);

    Map<String, Long> countMap = new HashMap<String, Long>();
    Map<String,String>  quchong=new HashMap<>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        List<Result> list = HBaseUtils.getRowsByColumns("total_service_company_personal_num", "sum_service_person", "cf", new String[]{"sum_service_person"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("sum_service_person".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.SUM_SERVICE_PERSON_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String sum_cnt=rs.getString("sum_cnt");
                    HBaseUtils.insert("total_service_company_personal_num","sum_service_person","cf","sum_service_person",sum_cnt);
                    countMap.put("sum_service_person",Long.valueOf(sum_cnt));
                }
            }catch (Exception e){
                logger.error("SumServicePersonResultBolt sql execute fail",e);
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
        Long sum_service_person = 0L;
        String id_card=tuple.getStringByField("id_card");

        try {
            if(!quchong.containsKey(id_card)){
                quchong.put(id_card,id_card);
                sum_service_person= countMap.get("sum_service_person")+1;
                countMap.put("sum_service_person",sum_service_person);
                for (String key : countMap.keySet()) {
                    HBaseUtils.insert("total_service_company_personal_num", key, "cf", "sum_service_person", countMap.get(key) + "");
                }
            }
        } catch (Exception e) {
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
