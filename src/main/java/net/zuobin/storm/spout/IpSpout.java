package net.zuobin.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

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
}
