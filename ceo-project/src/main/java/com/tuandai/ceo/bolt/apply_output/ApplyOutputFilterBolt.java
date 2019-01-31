package com.tuandai.ceo.bolt.apply_output;


import com.tuandai.tools.DateFmt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class ApplyOutputFilterBolt implements IBasicBolt {
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
            String massage=tuple.getString(0);
            if(massage!=null){
                //业务号\t时间(申请时间或者撮合时间)\t status(区分申请和撮合)
                String massageArr[]=massage.split("\\t");
                collector.emit(new Values(DateFmt.getCountDate(massageArr[1], DateFmt.date_month),massageArr[2]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cleanup() {

    }

    /**
     * date: 时间
     * status: 申请/撮合
     * num: 笔数
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date","status"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
