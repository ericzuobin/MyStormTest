package net.zuobin.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zuobin on 15/12/21.
 */
public class ProcessBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
         _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String ip = tuple.getString(0);

        Process process = null;
        List<String> processList = new ArrayList<String>();

        boolean suc = true;
        try {
            //前提是配置了SSH无密码登陆
            process = Runtime.getRuntime().exec("ssh "+ ip +" /usr/java/jdk1.7.0_79/bin/jps | grep \"\\(nimbus\\|core\\|drpc\\|supervisor\\|logviewer\\)\" | awk '{print $1 \"|\" $2 }'");

            //没有做异常处理,比如process的异常输出流造成的阻塞,
            //正常情况下应该处理标准输出和标准错误输出
            process.getErrorStream();
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                processList.add(line);
            }
            input.close();
        } catch (IOException e) {
            suc = false;
        }

        if (suc) {

            this._collector.emit(tuple , new Values(processList));
            this._collector.ack(tuple);
        }else {

            this._collector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("process"));

    }
}
