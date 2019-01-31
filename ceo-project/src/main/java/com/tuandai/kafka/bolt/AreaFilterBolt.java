package com.tuandai.kafka.bolt;


import com.tuandai.tools.DateFmt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class AreaFilterBolt implements IBasicBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String order=tuple.getString(0);
        if(order!=null){
            String orderArr[]=order.split(",");
            collector.emit(new Values(orderArr[3],orderArr[1], DateFmt.getCountDate(orderArr[2],DateFmt.date_short)));
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area_id","order_amt","order_date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
