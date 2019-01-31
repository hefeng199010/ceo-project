package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.borrow_limit.BorrowLimitFilterBolt;
import com.tuandai.ceo.bolt.borrow_limit.BorrowLimitResultBolt;
import com.tuandai.ceo.bolt.borrow_limit.BorrowLimitSumBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 借款期限占比分布
 */
public class BorrowLimitTopo {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new CommonSpout(KafkaTopic.FULL_ISSUE_PUSH.getValue(),"borrowLimit"), 2);
        builder.setBolt("borrowLimitFilterBolt", new BorrowLimitFilterBolt(), 2).shuffleGrouping("spout");
        builder.setBolt("limitBolt", new BorrowLimitSumBolt(), 2).fieldsGrouping("borrowLimitFilterBolt", new Fields("limit"));
        builder.setBolt("borrowLimitResultBolt", new BorrowLimitResultBolt(), 1).shuffleGrouping("limitBolt");
        Config conf = new Config();
        conf.setDebug(false);


        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("borrow_limit_topology", conf, builder.createTopology());
        }


    }
}
