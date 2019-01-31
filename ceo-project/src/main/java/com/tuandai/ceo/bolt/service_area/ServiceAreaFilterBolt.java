package com.tuandai.ceo.bolt.service_area;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class ServiceAreaFilterBolt implements IBasicBolt {
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
        String massage=tuple.getString(0);
        System.err.println(massage+"      ************");
        try {
            if(massage!=null){
                //业务号\t客户姓名\t客户类型\t  common_col（身份证号,营业执照号,统一社会信用代码）\t所属行业\t借款人所属省份\
                String massageArr[]=massage.split("\\t");
                collector.emit(new Values(massageArr[3], massageArr[5]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cleanup() {

    }

    /**
     * area: 省份
     * num: 笔数
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id_card","area"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
