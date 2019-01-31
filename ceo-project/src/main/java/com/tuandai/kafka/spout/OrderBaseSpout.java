package com.tuandai.kafka.spout;


import com.tuandai.kafka.consumer.OrderConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

public class OrderBaseSpout implements IRichSpout {
    TopologyContext context=null;
    SpoutOutputCollector collector=null;
    private Queue<String> queue=new LinkedBlockingDeque<>();
    private  String topic;
    private String group_id;
    public OrderBaseSpout(String topic) {
        this.topic=topic;
        this.group_id=group_id;
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.context=context;
        this.collector=collector;
        OrderConsumer orderConsumer=new OrderConsumer(topic,group_id);
        orderConsumer.start();
        queue=orderConsumer.getQueue();
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if(queue.size()>0){
            String str=queue.poll();
            collector.emit(new Values(str));
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
