package net.zuobin.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import net.zuobin.storm.bolt.ContentBolt;
import net.zuobin.storm.bolt.ProcessBolt;
import net.zuobin.storm.spout.IpSpout;

/**
 * Created by zuobin on 15/12/21.
 */
public class WordTopology  {

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("ipSpout" , new IpSpout(), 2);
        builder.setBolt("processBolt" , new ProcessBolt() ,5).shuffleGrouping("ipSpout");
        builder.setBolt("contentBolt" , new ContentBolt(),5).shuffleGrouping("processBolt");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }else {

            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("test", conf, builder.createTopology());

            Utils.sleep(60000);
            localCluster.killTopology("test");
            localCluster.shutdown();
        }
    }
}
