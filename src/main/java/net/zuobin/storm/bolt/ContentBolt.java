package net.zuobin.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zuobin on 15/12/22.
 */
public class ContentBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        List<String> list = (List<String>) tuple.getValues().get(0);

        Map<String,Integer> processMap = new HashMap<String, Integer>();

        for (String ss : list) {
            String[] processStr = ss.split("\\|");

            String key = processStr[1].trim();

            if (processMap.get(key)!=null) {
                int sum = processMap.get(key);
                processMap.put(key,sum++);
            }else {
                processMap.put(key, 1);
            }
        }

        //做其他的操作
       this._collector.emit(tuple,new Values(processMap));
       this._collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("content"));
    }
}
