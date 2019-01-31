package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.service_customer.ServiceCustomerFilterBolt;
import com.tuandai.ceo.bolt.service_customer.ServiceCustomerResultBolt;
import com.tuandai.ceo.bolt.service_customer.ServiceCustomerSumBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 服务客户行业分布
 */
public class ServiceCustormTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.APPROVAL_FINISHED.getValue(),"serviceCustorm"),2);
        builder.setBolt("serviceCustomerFilterBolt",new ServiceCustomerFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("serviceCustomerBolt", new ServiceCustomerSumBolt(), 2).fieldsGrouping("serviceCustomerFilterBolt", new Fields("profession"));
        builder.setBolt("serviceCustomerResultBolt",new ServiceCustomerResultBolt(),1).shuffleGrouping("serviceCustomerBolt");
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
            localCluster.submitTopology("service_customer_topology", conf, builder.createTopology());
        }


    }
}
