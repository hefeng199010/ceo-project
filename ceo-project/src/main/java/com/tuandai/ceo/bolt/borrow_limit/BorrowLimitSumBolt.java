package com.tuandai.ceo.bolt.borrow_limit;


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

public class BorrowLimitSumBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(BorrowLimitSumBolt.class);
    ConcurrentHashMap<String, Integer> countMap = new ConcurrentHashMap<String, Integer>();
    TopologyContext context = null;
    String today = null;

    @Override
    public void prepare(Map map, TopologyContext context) {
        List<Result> list = HBaseUtils.getRowsByColumns("borrow_limit_spread", "", "cf", new String[]{"sum_limit_num"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("sum_limit_num".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.BORROW_LIMIT_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String borrow_limit=rs.getString("borrow_limit");
                    String count=rs.getString("cnt");
                    HBaseUtils.insert("borrow_limit_spread",borrow_limit,"cf","sum_limit_num",count);
                    countMap.put(borrow_limit,Integer.valueOf(count));
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
        Integer num=0;
        try {
            String business_id = tuple.getStringByField("business_id");
            String limit = tuple.getStringByField("limit");

            if (countMap.get(limit) == null) {
                countMap.put(limit, 0);
            } else {
                num += countMap.get(limit)+1;
                countMap.put(limit, num);
            }

            collector.emit(new Values(limit, num));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("limit", "limit_num"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
