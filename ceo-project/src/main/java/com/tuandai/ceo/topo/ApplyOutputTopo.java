package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.apply_output.ApplyOutputFilterBolt;
import com.tuandai.ceo.bolt.apply_output.ApplyOutputResultBolt;
import com.tuandai.ceo.bolt.apply_output.ApplyOutputSumBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 客户撮合情况
 */
public class ApplyOutputTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.APPLY_OUTPUT.getValue(),"applyOutput"),2);
        builder.setBolt("applyOutputFilterBolt",new ApplyOutputFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("applyOutputBolt", new ApplyOutputSumBolt(), 2).fieldsGrouping("applyOutputFilterBolt", new Fields("date","status"));
        builder.setBolt("applyOutResultBolt",new ApplyOutputResultBolt(),1).shuffleGrouping("applyOutputBolt");
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
            localCluster.submitTopology("apply_output_topology", conf, builder.createTopology());
        }


    }
}
