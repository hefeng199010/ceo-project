package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.business_balance.BusinessBalanceFilterBolt;
import com.tuandai.ceo.bolt.business_balance.BusinessBalanceResultBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 撮合业务余额
 */
public class BusinessBalanceTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.BUSINESS_BALANCE.getValue(),"businessBalance"),2);
        builder.setBolt("sumServicePersonFilterBolt",new BusinessBalanceFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("sumServicePersonResultBolt",new BusinessBalanceResultBolt(),1).shuffleGrouping("sumServicePersonFilterBolt");
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
            localCluster.submitTopology("business_balance_topology", conf, builder.createTopology());
        }


    }
}
