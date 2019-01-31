package com.tuandai.ceo.bolt.borrow_limit;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BorrowLimitResultBolt implements IBasicBolt {

    ConcurrentHashMap<String,Integer> countMap=new ConcurrentHashMap<String,Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String limit = tuple.getStringByField("limit");
            int sum_limit_num = tuple.getIntegerByField("limit_num");
            countMap.put(limit,sum_limit_num);
            for(String key : countMap.keySet()){
                HBaseUtils.insert("borrow_limit_spread",key,"cf","sum_limit_num",countMap.get(key)+"");
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
