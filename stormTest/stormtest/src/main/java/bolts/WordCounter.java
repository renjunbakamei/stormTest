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
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounter implements IRichBolt{

    Integer id;

    String name;

    Map<String,Integer> counters;

    private OutputCollector collector;

    /**
     *初始化
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters=new HashMap<String,Integer>();
        this.collector=collector;
        this.name=topologyContext.getThisComponentId();
        this.id=topologyContext.getThisTaskId();
    }

    /**
     *为每个单词计数
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String str=tuple.getString(0);
        /**
         *如果单词尚不存在于map,我们就创建一个,如果已经存在,我们就为他加1
         */
        if(!counters.containsKey(str)){
            counters.put(str,1);
        }else{
            Integer c=counters.get(str)+1;
            counters.put(str,c);
        }
        //对元组作为应答
        collector.ack(tuple);
    }

    /**
     *这个spout结束时候（集群关闭的时候），我们会显示单词数量
     */
    @Override
    public void cleanup() {
        System.out.println("--单词数["+name+"-"+id+"]--");
        for(Map.Entry<String,Integer> entry:counters.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
