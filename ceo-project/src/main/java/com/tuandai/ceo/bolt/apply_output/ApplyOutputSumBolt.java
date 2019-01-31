package com.tuandai.ceo.bolt.apply_output;


import com.tuandai.ceo.enums.PrepareSql;
import com.tuandai.tools.DateFmt;
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

public class ApplyOutputSumBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(ApplyOutputSumBolt.class);
    ConcurrentHashMap<String, Long> countMap = new ConcurrentHashMap<String, Long>();

    @Override
    public void prepare(Map map, TopologyContext context) {
        List<Result> list = HBaseUtils.getRowsByColumns("apply_output_get_num", "", "cf", new String[]{"date_status"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("date_status".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.APPLY_OUT_PUT_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String cnt=rs.getString("cnt");
                    String date_time=rs.getString("date_time");
                    String status_type=rs.getString("status_type");
                    HBaseUtils.insert("apply_output_get_num", DateFmt.getCountDate(date_time,DateFmt.date_month)+"_"+status_type,"cf","date_status",cnt);
                    countMap.put(DateFmt.getCountDate(date_time,DateFmt.date_month)+"_"+status_type,Long.valueOf(cnt));
                }
            }catch (Exception e){
                logger.error("ApplyOutputSumBolt sql execute fail",e);
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
        try {
            Long sum=0L;
            String date = tuple.getStringByField("date");
            String status = tuple.getStringByField("status");

            if (countMap.get(date + "_" + status) == null) {
                countMap.put(date + "_" + status, 1L);
            } else {
                sum += countMap.get(date + "_" + status)+1;
                countMap.put(date + "_" + status, sum);
            }

            collector.emit(new Values(date + "_" + status, sum));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date_status", "num"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
