/*
 * Copyright 2017 jfpal.com All right reserved. This software is the
 * confidential and proprietary information of jfpal.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with jfpal.com.
 
 Created by jun.ren on 2017/3/27.
 
 */
package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordNormalizer implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=collector;
    }

    /**
     *bolt从单词文件接收到文本行，并标准化它。
     * 文本行会全部转化成小写，并且切分它。从中得到所有的单ci
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String sentence=tuple.getString(0);
        String[] words=sentence.split(" ");
        for(String word:words){
            word=word.trim();
            if(!word.isEmpty()){
                word=word.toLowerCase();
                //发布这个单词
                List a=new ArrayList();
                a.add(tuple);
                collector.emit(a,new Values(word));
            }
        }
        //对元组进行应答
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    /**
     *这个bolt只会发布word域
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
