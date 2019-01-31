package com.tuandai.kafka.bolt;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class AreaResultBolt implements IBasicBolt {

    Map<String,Double> countMap=new HashMap<String,Double>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        double sum=0;
        String date_area=tuple.getStringByField("date_area");
        double amount=tuple.getDoubleByField("amount");
        System.err.println("开始:"+date_area);
        countMap.put(date_area,amount);
        for(String key : countMap.keySet()){
//                result_amt+=countMap.get(key);
            //把结果写入hbase
            //2015-05-05,amt
            HBaseUtils.insert("area_order",key,"cf","order_amount",countMap.get(key)+"");
            System.err.println("areaResultBolt  :"+ "rowkey:"+key+"family:cf"+"   quality:order_amount:"+countMap.get(key));
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
