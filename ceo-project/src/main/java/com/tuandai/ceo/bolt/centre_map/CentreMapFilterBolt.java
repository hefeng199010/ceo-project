package com.tuandai.ceo.bolt.centre_map;


import com.tuandai.tools.DateFmt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class CentreMapFilterBolt implements IBasicBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t放款日期\t业务类型\t放款金额\t公司\t借款期限\t客户姓名\t省份\t借款人\t借款金额\t行业分布
        try {
            String massage=tuple.getString(0);
            System.err.println("中心地图分布:"+massage);
            if(massage!=null){
                //业务号\t标号\t满标金额(财务登记出款金额)\t满标时间\t业务类型\t分公司\t借款期限\t业务所属省份\t主借款人名字\t是否是主借款人(1:是,0:否)
                String massageArr[]=massage.split("\\t");
                //如果是 主借款人，就发送
                    collector.emit(new Values(massageArr[0], massageArr[7],massageArr[2],DateFmt.getCountDate(massageArr[3], DateFmt.date_long),massageArr[5],massageArr[8]));


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
        declarer.declare(new Fields("business_id","area","amount","borrow_date","company","borrow_name"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
