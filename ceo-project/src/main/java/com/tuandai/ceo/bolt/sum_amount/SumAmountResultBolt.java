package com.tuandai.ceo.bolt.sum_amount;

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
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SumAmountResultBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(SumAmountResultBolt.class);

    Map<String,Double> countMap=new HashMap<String,Double>();
    java.text.NumberFormat nf=null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        List<Result> list = HBaseUtils.getRowsByColumns("total_out_put_money", "sum_amount", "cf", new String[]{"sum_amount"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("sum_amount".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.SUM_AMOUNT_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
               conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String total_amont=rs.getString("total_sum_amt");
                    HBaseUtils.insert("total_out_put_money","sum_amount","cf","sum_amount",total_amont);
                    countMap.put("sum_amount",Double.valueOf(total_amont));
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
    Double sum=0.0;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
//        double amount =0;
//        String order_date = tuple.getStringByField("order_date");
//        String business_id =  tuple.getStringByField("business_id");
        double order_amt = tuple.getDoubleByField("order_amt");
//        CommonDoubleMap.sumValueAll(countMap,"sum_amount",order_amt);

        if(countMap.get("sum_amount")!=null){
            sum=order_amt+countMap.get("sum_amount");
            countMap.put("sum_amount",sum);
        }else{
            countMap.put("sum_amount",0.0);
        }


        for(String key : countMap.keySet()){
            //把结果写入hbase
            HBaseUtils.insert("total_out_put_money",key,"cf","sum_amount",nf.format(countMap.get(key)));
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
