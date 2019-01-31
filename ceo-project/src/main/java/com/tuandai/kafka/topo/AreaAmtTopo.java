package com.tuandai.kafka.topo;


import com.tuandai.kafka.bolt.AreaAmtBolt;
import com.tuandai.kafka.bolt.AreaFilterBolt;
import com.tuandai.kafka.bolt.AreaResultBolt;
import com.tuandai.kafka.spout.OrderBaseSpout;
import com.tuandai.tools.KafkaProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class AreaAmtTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new OrderBaseSpout(KafkaProperties.TOPIC),1);
        builder.setBolt("filterBolt",new AreaFilterBolt(),5).shuffleGrouping("spout");
        builder.setBolt("areaBolt",new AreaAmtBolt(),4).fieldsGrouping("filterBolt",new Fields("area_id","order_date"));
        builder.setBolt("resultBolt",new AreaResultBolt(),1).shuffleGrouping("areaBolt");
//        Map conf = new HashMap();
//        conf.put(Config.TOPOLOGY_WORKERS, 4);
        Config conf=new Config();
        conf.setDebug(false);


        if(args.length>0){
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        else {
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }


    }
}
