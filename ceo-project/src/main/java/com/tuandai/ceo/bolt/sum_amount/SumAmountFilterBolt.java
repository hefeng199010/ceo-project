package com.tuandai.ceo.bolt.sum_amount;


import com.tuandai.tools.DateFmt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class SumAmountFilterBolt implements IBasicBolt {

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t放款日期\t业务类型\t放款金额\t公司\t借款期限\t客户姓名\t省份\t借款人\t借款金额\t行业分布
        String massage = tuple.getString(0);
        System.err.println("累计撮合金额:  "+massage);
        try {
            if (massage != null) {
                String massageArr[] = massage.split("\\t");
                collector.emit(new Values(massageArr[0], Double.parseDouble(massageArr[2]), DateFmt.getCountDate(massageArr[3], DateFmt.date_short)));
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("business_id", "order_amt", "order_date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
