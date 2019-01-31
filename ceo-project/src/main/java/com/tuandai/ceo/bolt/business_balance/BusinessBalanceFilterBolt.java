package com.tuandai.ceo.bolt.business_balance;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class BusinessBalanceFilterBolt implements IBasicBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String massage=tuple.getString(0);
            System.err.println("撮合业务余额:   "+massage);
            if(massage!=null){
                //business_id\t已还本金
                String massageArr[]=massage.split("\\t");
                collector.emit(new Values(massageArr[0], massageArr[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("business_id","amount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
