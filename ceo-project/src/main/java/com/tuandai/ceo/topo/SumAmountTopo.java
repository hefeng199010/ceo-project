package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.sum_amount.SumAmountFilterBolt;
import com.tuandai.ceo.bolt.sum_amount.SumAmountResultBolt;
import com.tuandai.ceo.spout.CommonSpout;
import com.tuandai.ceo.enums.KafkaTopic;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 累计撮合金额
 */
public class SumAmountTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.FULL_ISSUE_PUSH.getValue(),"sumAmount1"),1);
        builder.setBolt("sumAmountFilterBolt",new SumAmountFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("sumAmountResultBolt",new SumAmountResultBolt(),1).shuffleGrouping("sumAmountFilterBolt");
        Config conf=new Config();
        conf.setDebug(false);
        conf.setNumWorkers(3);

        if(args.length>0){
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }   catch (Exception e) {
            e.printStackTrace();
        }
        }
        else {
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("sum_amount_topology", conf, builder.createTopology());
        }


    }
}
