package com.tuandai.ceo.bolt.month_amount;


import com.tuandai.ceo.enums.PrepareSql;
import com.tuandai.ceo.utils.CommonDoubleMap;
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

public class MonthAmountSumBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(MonthAmountSumBolt.class);
    ConcurrentHashMap<String, Double> countMap = new ConcurrentHashMap<String, Double>();
    TopologyContext context = null;
    String today_month = null;

    @Override
    public void prepare(Map map, TopologyContext context) {
        this.context = context;
        today_month = DateFmt.getCountDate(null, DateFmt.date_month);
        countMap = this.initMap(today_month);
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.MONTH_AMOUNT_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String sum_amt=rs.getString("sum_amt");
                    String date_time=rs.getString("date_time");
                    String business_type=rs.getString("business_type");
                    HBaseUtils.insert("every_month_out_put_money_trend",DateFmt.getCountDate(date_time,DateFmt.date_month)+"_"+business_type,"cf","date_type_amount",sum_amt);
                    countMap.put(DateFmt.getCountDate(date_time,DateFmt.date_month)+"_"+business_type,Double.valueOf(sum_amt));
                }
                conn.close();
                rs.close();
            }catch (Exception e){
                logger.error("SumAmountResultBolt sql execute fail",e);
            }
        }
    }

    private ConcurrentHashMap<String, Double> initMap(String today_month) {
        List<Result> list = HBaseUtils.getRowsByColumns("every_month_out_put_money_trend",today_month,"cf",new String[]{"date_type_amount"});
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("date_type_amount".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        return countMap;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            double amount=0;
            String business_type = tuple.getStringByField("type");
            String order_amt = tuple.getStringByField("order_amt");
            String order_date = tuple.getStringByField("order_date");
            String company = tuple.getStringByField("company");

            if (!order_date.equals(today_month)) {
                countMap.clear();
            }

            if(countMap.get(order_date+"_"+business_type)==null){
                amount=Double.parseDouble(order_amt);
                countMap.put(order_date+"_"+business_type,Double.parseDouble(order_amt));
            }
            else{
                amount+=countMap.get(order_date+"_"+business_type)+Double.parseDouble(order_amt);
                countMap.put(order_date+"_"+business_type,amount);
            }

            // CommonDoubleMap.sumValue(countMap,order_date+"_"+type,Double.parseDouble(order_amt));
            collector.emit(new Values(order_date + "_" + business_type, countMap.get(order_date+"_"+business_type)));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date_type", "amount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
