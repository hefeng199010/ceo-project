package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.sum_service_person.SumServicePersonFilterBolt;
import com.tuandai.ceo.bolt.sum_service_person.SumServicePersonResultBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 累计服务企业与个人
 */
public class SumServicePersonTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.APPROVAL_FINISHED.getValue(),"sumServicePerson"),2);
        builder.setBolt("sumServicePersonFilterBolt",new SumServicePersonFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("sumServicePersonResultBolt",new SumServicePersonResultBolt(),1).shuffleGrouping("sumServicePersonFilterBolt");
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
            localCluster.submitTopology("sum_service_person_topology", conf, builder.createTopology());
        }


    }
}
