package com.tuandai.ceo.bolt.service_customer;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceCustomerResultBolt implements IBasicBolt {

    ConcurrentHashMap<String,Integer> countMap=new ConcurrentHashMap<String,Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String job = tuple.getStringByField("profession");
        int num = tuple.getIntegerByField("num");
        try {
            countMap.put(job,num);
            for(String key : countMap.keySet()){
                HBaseUtils.insert("service_custorm_profession_spread",key,"cf","profession",countMap.get(key)+"");
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
