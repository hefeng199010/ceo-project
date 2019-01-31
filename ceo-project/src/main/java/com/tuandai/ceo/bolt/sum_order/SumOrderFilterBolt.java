package com.tuandai.ceo.bolt.sum_order;


import com.tuandai.tools.DateFmt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;


public class SumOrderFilterBolt implements IBasicBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t放款日期\t业务类型\t放款金额\t公司\t借款期限\t客户姓名\t省份\t借款人\t借款金额\t行业分布
        String massage=tuple.getString(0);
        System.err.println("累计撮合单数:  "+massage);
        try {
            if(massage!=null){
                String massageArr[]=massage.split("\\t");
                if(massageArr[0]!=null){
                    collector.emit(new Values(massageArr[0], DateFmt.getCountDate(massageArr[4], DateFmt.date_short),1));
                }
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
        declarer.declare(new Fields("business_id","order_date","order_num"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
