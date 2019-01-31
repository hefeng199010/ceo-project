package com.tuandai.kafka.bolt;


import com.tuandai.tools.DateFmt;
import com.tuandai.tools.HBaseUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AreaAmtBolt implements IBasicBolt {
    Map<String,Double> countMap=new HashMap<String,Double>();
    TopologyContext context=null;
    String today=null;
    @Override
    public void prepare(Map map, TopologyContext context) {
        this.context=context;
        //支持重启
        //根据hbase的初始值进行初始化
        today= DateFmt.getCountDate(null,DateFmt.date_short);
        countMap=this.initMap(today);

    }

    private Map<String,Double> initMap(String today) {
        Map<String,Double> map=new HashMap<String,Double>();
        List<Result> list = HBaseUtils.getRowsByColumns("area_order",today,"cf",new String[]{"order_amount"});
       // List<Result> list= HBaseUtils.getRowsColumns("area_order",today,new String[]{"order_amount"});
        for(Result rs : list){
            String rowkey=new String(rs.getRow());
            for(KeyValue keyValue : rs.raw()){
                if("order_amount".equals(new String(keyValue.getQualifier()))){
                    System.err.println(rowkey+"   *****  " +new String(keyValue.getQualifier())
                            +"      "+new String(keyValue.getValue()));
                    map.put(rowkey,Double.parseDouble(new String(keyValue.getValue())));
                }
            }
        }
        return map;

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        double amount=0;
        String area_id=tuple.getStringByField("area_id");
        String order_amt=tuple.getStringByField("order_amt");
        String order_date=tuple.getStringByField("order_date");
//        System.err.println("taskID="+context.getThisTaskId()+"  area_id="+area_id+
//          "      order_amt="+order_amt+"    order_date="+order_date+"    amount="+amount);
        if(!order_date.equals(today)){
            countMap.clear();
        }
        if(countMap.get(order_date+"_"+area_id)==null){
            amount=Double.parseDouble(order_amt);
            countMap.put(order_date+"_"+area_id,Double.parseDouble(order_amt));
        }
        else{
            amount+=countMap.get(order_date+"_"+area_id)+Double.parseDouble(order_amt);
            countMap.put(order_date+"_"+area_id,amount);
        }
        //System.err.println("taskID="+context.getThisTaskId()+"   AreaAmtBolt   .....   "+countMap);
       // collector.emit(new Values(amount));
//        System.err.println("taskID="+context.getThisTaskId()+"  area_id="+area_id+
              //  "      order_amt="+order_amt+"    order_date="+order_date);
//        System.err.println("area_id="+area_id+",       amount="+amount);
        collector.emit(new Values(order_date+"_"+area_id,amount));
    }



    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date_area","amount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
