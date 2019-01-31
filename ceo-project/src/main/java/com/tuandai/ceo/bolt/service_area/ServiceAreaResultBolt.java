package com.tuandai.ceo.bolt.service_area;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceAreaResultBolt implements IBasicBolt {

    ConcurrentHashMap<String,Integer> countMap=new ConcurrentHashMap<String,Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String area = tuple.getStringByField("area");
        int num = tuple.getIntegerByField("num");
        try {
            countMap.put(area,num);
            for(String key : countMap.keySet()){
                HBaseUtils.insert("service_custom_area_spread",key,"cf","area",countMap.get(key)+"");
                //System.err.println("areaResultBolt  :"+ "rowkey:"+key+"family:cf"+"   quality:order_amount:"+countMap.get(key));
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
