package com.tuandai.ceo.bolt.borrow_limit;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class BorrowLimitFilterBolt implements IBasicBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    /**
     *
     * @param tuple
     * 业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t放款日期\t业务类型\t放款金额\t公司\t借款期限\t客户姓名\t省份\t借款人\t借款金额\t行业分布
     * @param collector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String message=tuple.getString(0);
            System.err.println("借款期限占比分布:  "+message);
            if(message!=null){
                //业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t业务类型\t分公司\t借款期限\t业务所属省份\t主借款人名字\t是否是主借款人(1:是,0:否)
                String massageArr[]=message.split("\\t");
                collector.emit(new Values(massageArr[0], massageArr[6]));
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
        declarer.declare(new Fields("business_id","limit"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
