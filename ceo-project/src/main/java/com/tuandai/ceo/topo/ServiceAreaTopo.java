package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.service_area.ServiceAreaFilterBolt;
import com.tuandai.ceo.bolt.service_area.ServiceAreaResultBolt;
import com.tuandai.ceo.bolt.service_area.ServiceAreaSumBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 服务客户地区分布top10
 */
public class ServiceAreaTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.APPROVAL_FINISHED.getValue(),"serviceArea"),2);
        builder.setBolt("serviceAreaFilterBolt",new ServiceAreaFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("serviceAreaBolt", new ServiceAreaSumBolt(), 2).fieldsGrouping("serviceAreaFilterBolt", new Fields("area"));
        builder.setBolt("serviceAreaResultBolt",new ServiceAreaResultBolt(),1).shuffleGrouping("serviceAreaBolt");
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
            localCluster.submitTopology("service_area_topology", conf, builder.createTopology());
        }


    }
}
