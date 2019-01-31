package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.month_amount.MonthAmountFilterBolt;
import com.tuandai.ceo.bolt.month_amount.MonthAmountResultBolt;
import com.tuandai.ceo.bolt.month_amount.MonthAmountSumBolt;
import com.tuandai.ceo.bolt.month_amount.TestBolt;
import com.tuandai.ceo.spout.CommonSpout;
import com.tuandai.ceo.enums.KafkaTopic;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 各月撮合金额趋势
 */
public class MonthAmountTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.FULL_ISSUE_PUSH.getValue(),"monthAmount"),2);
        builder.setBolt("monthAmountFilterBolt",new MonthAmountFilterBolt(),2).shuffleGrouping("spout");
//        builder.setBolt("", new TestBolt(), 1).shuffleGrouping("monthAmountFilterBolt");
        builder.setBolt("monthAmtBolt",new MonthAmountSumBolt(),2).fieldsGrouping("monthAmountFilterBolt",new Fields("order_date","type"));
        builder.setBolt("monthAmountResultBolt",new MonthAmountResultBolt(),1).shuffleGrouping("monthAmtBolt");
        Config conf=new Config();
        conf.setDebug(false);


        if(args.length>0){
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }   catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("MonthAmountTopo", conf, builder.createTopology());
        }


    }
}
