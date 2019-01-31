package com.tuandai.ceo.bolt.centre_map;

import com.tuandai.tools.HBaseUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CentreMapResultBolt implements IBasicBolt {

    ConcurrentHashMap<String, Double> countMap = new ConcurrentHashMap<String, Double>();
    java.text.NumberFormat nf=null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String area = tuple.getStringByField("area");
        double num = tuple.getDoubleByField("num");
        String borrow_date = tuple.getStringByField("borrow_date");
        String borrow_name = tuple.getStringByField("borrow_name");
        double borrow_amount = tuple.getDoubleByField("borrow_amount");
        String company=tuple.getStringByField("company");
        countMap.put(area, num);
        try {
            for (String key : countMap.keySet()) {
                HBaseUtils.insert("center_map", key, "cf", "area", nf.format(countMap.get(key)));
            }
            HBaseUtils.addData("center_map",borrow_date,"cf",new String[]{"borrow_date","company","borrow_name", "borrow_amount","province"},new String[]{borrow_date,company,borrow_name,nf.format(borrow_amount),area});
        } catch (IOException e) {
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
