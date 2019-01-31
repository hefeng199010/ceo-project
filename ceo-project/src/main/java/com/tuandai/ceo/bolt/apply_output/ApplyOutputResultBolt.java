package com.tuandai.ceo.bolt.apply_output;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ApplyOutputResultBolt implements IBasicBolt {

    ConcurrentHashMap<String,Long> countMap=new ConcurrentHashMap<String,Long>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String date_status = tuple.getStringByField("date_status");
            Long num = tuple.getLongByField("num");
            countMap.put(date_status,num);
            for(String key : countMap.keySet()){
                HBaseUtils.insert("apply_output_get_num",key,"cf","date_status",countMap.get(key)+"");
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
