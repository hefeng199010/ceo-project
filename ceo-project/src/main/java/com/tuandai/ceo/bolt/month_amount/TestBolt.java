package com.tuandai.ceo.bolt.month_amount;

import com.tuandai.ceo.enums.PrepareSql;
import com.tuandai.tools.DateFmt;
import com.tuandai.tools.HBaseUtils;
import com.tuandai.tools.JdbcProperties;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestBolt implements IBasicBolt {
    protected static Logger logger = Logger.getLogger(MonthAmountSumBolt.class);
    ConcurrentHashMap<String, Double> countMap = new ConcurrentHashMap<String, Double>();
    TopologyContext context = null;
    String today_month = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        this.context = context;

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        double amount=0;
        String business_type = tuple.getStringByField("type");
        String order_amt = tuple.getStringByField("order_amt");
        String order_date = tuple.getStringByField("order_date");
        String company = tuple.getStringByField("company");
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
