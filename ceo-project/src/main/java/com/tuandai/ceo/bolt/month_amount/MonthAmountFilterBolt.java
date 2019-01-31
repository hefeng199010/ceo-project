package com.tuandai.ceo.bolt.month_amount;


import com.tuandai.tools.BusinessType;
import com.tuandai.tools.DateFmt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class MonthAmountFilterBolt implements IBasicBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t放款日期\t业务类型\t放款金额\t公司\t借款期限\t客户姓名\t省份\t借款人\t借款金额\t行业分布
       //业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t业务类型\t分公司\t借款期限\t业务所属省份\t主借款人名字\t是否是主借款人(1:是,0:否)
        String order=tuple.getString(0);
        System.err.println("各月金额撮合趋势:"+order);
        try {
            if(order!=null){
                String orderArr[]=order.split("\\t");
                System.err.println(DateFmt.getCountDate(orderArr[3], DateFmt.date_month)+"       ***********   "+BusinessType.getType(orderArr[4]));
                collector.emit(new Values(orderArr[5],orderArr[0], BusinessType.getType(orderArr[4]), orderArr[2], DateFmt.getCountDate(orderArr[3], DateFmt.date_month)));
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
        declarer.declare(new Fields("company","business_id","type","order_amt","order_date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
