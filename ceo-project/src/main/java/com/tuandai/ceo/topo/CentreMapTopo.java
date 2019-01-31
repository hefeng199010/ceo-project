package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.centre_map.CentreMapFilterBolt;
import com.tuandai.ceo.bolt.centre_map.CentreMapResultBolt;
import com.tuandai.ceo.bolt.centre_map.CentreMapSumBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 中心地图
 */
public class CentreMapTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.FULL_ISSUE_PUSH.getValue(),"centreMap"),2);
        builder.setBolt("centreMapFilterBolt",new CentreMapFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("centreMapSumBolt",new CentreMapSumBolt(),2).fieldsGrouping("centreMapFilterBolt",new Fields("area"));
        builder.setBolt("centreMapResultBolt",new CentreMapResultBolt(),1).shuffleGrouping("centreMapSumBolt");
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
            localCluster.submitTopology("centre_map_topology", conf, builder.createTopology());
        }


    }
}
