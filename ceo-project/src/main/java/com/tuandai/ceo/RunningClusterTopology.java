package com.tuandai.ceo;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class RunningClusterTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        conf.put(Config.NIMBUS_HOST, "172.16.200." +
                "114");
        conf.setDebug(true);
        Map storm_conf = Utils.readStormConfig();
        storm_conf.put("nimbus.host", "172.16.200.114");
        Nimbus.Client client = NimbusClient.getConfiguredClient(storm_conf)
                .getClient();
        String inputJar = "D:\\workcode\\svn\\ceo-project\\target\\ceo-project-1.0-SNAPSHOT-jar-with-dependencies.jar";
        NimbusClient nimbus = new NimbusClient(storm_conf, "172.16.200.114",
                6627);
        // upload topology jar to Cluster using StormSubmitter
        String uploadedJarLocation = StormSubmitter.submitJar(storm_conf,
                inputJar);
        try {
            String jsonConf = JSONValue.toJSONString(storm_conf);
            nimbus.getClient().submitTopology("sumServicePersonTopo",
                    uploadedJarLocation, jsonConf, builder.createTopology());
        } catch (AlreadyAliveException ae) {
            ae.printStackTrace();
        }
        Thread.sleep(60000);
    }
}