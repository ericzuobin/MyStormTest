package net.zuobin.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zuobin on 15/12/21.
 */
public class IpSpout extends BaseRichSpout{

    SpoutOutputCollector _outOutputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("ip"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _outOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

        Utils.sleep(10000);

        //获取数据源,这里举例直接new一个
        List<String> ipList = new ArrayList<String>();
        ipList.add("sahinn@172.16.3.215");

        for (String s : ipList) {
            _outOutputCollector.emit(new Values(s));
        }
    }

    public static void main(String[] args) throws Exception{

        Process process = null;
        List<String> processList = new ArrayList<String>();
        process = Runtime.getRuntime().exec("ssh sahinn@172.16.22.250 /usr/java/jdk1.7.0_79/bin/jps | grep \"\\(nimbus\\|core\\|drpc\\|supervisor\\|logviewer\\)\" | awk '{print $1 \"|\" $2 }'");
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        while ((line = input.readLine()) != null) {
            processList.add(line);
        }
        input.close();

        for (String li : processList) {
            System.out.println(li);
        }     }
}
