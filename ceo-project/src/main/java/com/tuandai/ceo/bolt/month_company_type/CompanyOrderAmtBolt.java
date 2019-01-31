package com.tuandai.ceo.bolt.month_company_type;


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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompanyOrderAmtBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(CompanyOrderAmtBolt.class);
    Map<String,Double> countMap=new HashMap<>();
    TopologyContext context=null;
    String today=null;
    @Override
    public void prepare(Map map, TopologyContext context) {
        this.context=context;
        //支持重启
        //根据hbase的初始值进行初始化
        today= DateFmt.getCountDate(null, DateFmt.date_month);
        countMap=this.initMap(today);
        if(countMap.isEmpty()){
            //hbase 为空，从mysql导入
            String sql = PrepareSql.MONTH_COMPANY_TYPE_SQL.getSql();
            ResultSet rs = null;
            Connection conn=null;
            try{
                conn = JdbcProperties.ConnectMysql();
                Statement stmt =conn.createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()){
                    String company=rs.getString("company");
                    String sum_amt=rs.getString("sum_amt");
                    String business_type=rs.getString("business_type");
                    HBaseUtils.insert("every_month_branch_out_put_money_trend",today+"_"+company+"_"+business_type,"cf","date_company_type",sum_amt);
                    countMap.put(today+"_"+company+"_"+business_type,Double.valueOf(sum_amt));
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

    private ConcurrentHashMap<String,Double> initMap(String today) {
        ConcurrentHashMap<String,Double> map=new ConcurrentHashMap<String,Double>();
        List<Result> list = HBaseUtils.getRowsByColumns("every_month_branch_out_put_money_trend",today,"cf",new String[]{"order_amount"});
        for(Result rs : list){
            String rowkey=new String(rs.getRow());
            for (Cell cell : rs.listCells()) {
                if ("order_amount".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    countMap.put(rowkey, Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }
        return map;

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        double amount=0;
        String company = tuple.getStringByField("company");
//        String business_id = tuple.getStringByField("business_id");
        String type = tuple.getStringByField("type");
        String order_amt = tuple.getStringByField("order_amt");
        String order_date = tuple.getStringByField("order_date");

        if(!order_date.equals(today)){
            countMap.clear();
        }
        //CommonDoubleMap.sumValue(countMap,order_date+"_"+company+"_"+type,Double.parseDouble(order_amt));
        try {
            if(countMap.get(order_date+"_"+company+"_"+type)==null){
                amount = Double.parseDouble(order_amt);
                countMap.put(order_date+"_"+company+"_"+type,amount);
            }
            else{
                amount+=countMap.get(order_date+"_"+company+"_"+type)+Double.parseDouble(order_amt);
                countMap.put(order_date+"_"+company+"_"+type,amount);
            }

            collector.emit(new Values(order_date+"_"+company+"_"+type,countMap.get(order_date+"_"+company+"_"+type)));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }



    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date_company_type","amount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
