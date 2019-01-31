package com.tuandai.ceo.bolt.month_amount;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class MonthAmountResultBolt implements IBasicBolt {

    Map<String, Double> countMap = new HashMap<String, Double>();
    java.text.NumberFormat nf=null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
//        double sum=0;
        try {
            String date_type = tuple.getStringByField("date_type");
            double amount = tuple.getDoubleByField("amount");
            countMap.put(date_type, amount);
            for (String key : countMap.keySet()) {
                HBaseUtils.insert("every_month_out_put_money_trend", key, "cf", "date_type_amount", nf.format(countMap.get(key)));
    //            System.err.println("areaResultBolt  :"+ "rowkey:"+key+"family:cf"+"   quality:order_amount:"+countMap.get(key));
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
