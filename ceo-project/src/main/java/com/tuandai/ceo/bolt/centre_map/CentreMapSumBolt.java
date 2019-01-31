package com.tuandai.ceo.bolt.centre_map;


import com.tuandai.ceo.enums.PrepareSql;
import com.tuandai.ceo.utils.CommonDoubleMap;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CentreMapSumBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(CentreMapSumBolt.class);
    ConcurrentHashMap<String, Double> countMap = new ConcurrentHashMap<String, Double>();
    ConcurrentHashMap<Date, Double> dateMap = new ConcurrentHashMap<Date, Double>();

    @Override
    public void prepare(Map map, TopologyContext context) {
        List<Result> list = HBaseUtils.getRowsByColumns("center_map","","cf",new String[]{"area"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("area".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.CENTRE_MAP_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String total_sum_amt=rs.getString("total_sum_amt");
                    String province=rs.getString("province");
                    HBaseUtils.insert("center_map",province,"cf","area",total_sum_amt);
                    countMap.put(province,Double.valueOf(total_sum_amt));
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
        double sum_amount=0;
        try {
            String borrow_date = tuple.getStringByField("borrow_date");//借款时间
            String company=tuple.getStringByField("company");//分公司
            String borrow_name = tuple.getStringByField("borrow_name");//借款人名字
            String area = tuple.getStringByField("area");//省份
            double area_amount = Double.parseDouble(tuple.getStringByField("amount"));//借款金额

            //CommonDoubleMap.sumValue(countMap, area, area_amount);
            if(countMap.get(area)==null){
                sum_amount+=area_amount;
                countMap.put(area,area_amount);
            }
            else{
                sum_amount+=countMap.get(area)+area_amount;
                countMap.put(area,sum_amount);
            }
            collector.emit(new Values(area, countMap.get(area), borrow_date, borrow_name, area_amount,company));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area", "num", "borrow_date", "borrow_name", "borrow_amount","company"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
