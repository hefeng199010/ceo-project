package com.tuandai.ceo.bolt.month_company_type;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompanyOrderResultBolt implements IBasicBolt {

    ConcurrentHashMap<String, Double> countMap = new ConcurrentHashMap<String, Double>();
    java.text.NumberFormat nf=null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String date_company_type = tuple.getStringByField("date_company_type");
        double amount = tuple.getDoubleByField("amount");
        try {
            countMap.put(date_company_type, amount);
            for (String key : countMap.keySet()) {
    //                result_amt+=countMap.get(key);
                //把结果写入hbase
                //2015-05-05,amt
                HBaseUtils.insert("every_month_branch_out_put_money_trend", key, "cf", "date_company_type", nf.format(countMap.get(key)));
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
