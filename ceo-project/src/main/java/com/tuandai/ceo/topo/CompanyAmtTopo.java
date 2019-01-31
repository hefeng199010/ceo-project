package com.tuandai.ceo.topo;


import com.tuandai.ceo.bolt.month_company_type.CompanyOrderAmtBolt;
import com.tuandai.ceo.bolt.month_company_type.CompanyOrderFilterBolt;
import com.tuandai.ceo.bolt.month_company_type.CompanyOrderResultBolt;
import com.tuandai.ceo.enums.KafkaTopic;
import com.tuandai.ceo.spout.CommonSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 当月分公司撮合金额排名top10
 */
public class CompanyAmtTopo {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new CommonSpout(KafkaTopic.FULL_ISSUE_PUSH.getValue(),"companyAmt"),2);
        builder.setBolt("companyAmtFilterBolt",new CompanyOrderFilterBolt(),2).shuffleGrouping("spout");
        builder.setBolt("companyBolt",new CompanyOrderAmtBolt(),2).fieldsGrouping("companyAmtFilterBolt",new Fields("company","type"));
        builder.setBolt("resultBolt",new CompanyOrderResultBolt(),1).shuffleGrouping("companyBolt");
        Config conf=new Config();
        conf.setDebug(false);


        if(args.length>0){
            try {
//                LocalCluster cluster = new LocalCluster();
//                cluster.submitTopology("spout", conf, builder.createTopology());
//                cluster.killTopology("spout");
//                cluster.shutdown();
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }   catch (Exception e) {
            e.printStackTrace();
        }
        }
        else {
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("company_Amt_topology", conf, builder.createTopology());
        }


    }
}
