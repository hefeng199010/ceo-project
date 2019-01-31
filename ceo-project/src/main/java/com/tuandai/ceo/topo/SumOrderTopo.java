package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.sum_order.SumOrderFilterBolt;
import com.tuandai.ceo.bolt.sum_order.SumOrderResultBolt;
import com.tuandai.ceo.spout.CommonSpout;
import com.tuandai.ceo.enums.KafkaTopic;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 累计撮合单数
 */
public class SumOrderTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.FULL_ISSUE_PUSH.getValue(),"sumOrder"),2);
        builder.setBolt("sumOrderFilterBolt",new SumOrderFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("sumOrderResultBolt",new SumOrderResultBolt(),1).shuffleGrouping("sumOrderFilterBolt");
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
            localCluster.submitTopology("sum_order_topology", conf, builder.createTopology());
        }


    }
}
