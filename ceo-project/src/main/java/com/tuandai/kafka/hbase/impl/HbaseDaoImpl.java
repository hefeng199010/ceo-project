package com.tuandai.kafka.hbase.impl;


import com.tuandai.kafka.hbase.HbaseDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/3/6.
 */
public class HbaseDaoImpl implements HbaseDao {
    private HConnection htablePool=null;

    public HbaseDaoImpl() {
        Configuration conf=new Configuration();//对应hbase-site.xml
        String zk_list="172.16.200.114,172.16.200.122,172.16.200.123";
        conf.set("hbase.zookeeper.quorum",zk_list);
        try {
            htablePool = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void save(Put put, String tableName) {
        HTableInterface table=null;
        try {
            table = htablePool.getTable(tableName);
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void insert(String tableName, String rowKey, String family, String quality, String value) {
        HTableInterface table=null;
        try {
            table = htablePool.getTable(tableName);
            Put put=new Put(rowKey.getBytes());
            put.add(family.getBytes(),quality.getBytes(),value.getBytes());
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void save(List<Put> put, String tableName) {
        HTableInterface table=null;
        try {
            table = htablePool.getTable(tableName);
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Result getOneRowResult(String tableName, String rowKey) {
        HTableInterface table=null;
        Result result=null;
        try {
            table = htablePool.getTable(tableName);
            Get get=new Get(rowKey.getBytes());
            result=table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKey_like) {
        HTableInterface table=null;
        Result result=null;
        List<Result> list=new ArrayList<Result>();
        try {
            table = htablePool.getTable(tableName);
            //rowkey的模糊匹配
            PrefixFilter filter=new PrefixFilter(rowKey_like.getBytes());
            Scan scan=new Scan();
            scan.setFilter(filter);
            ResultScanner resultScanner=table.getScanner(scan);
            for(Result rs : resultScanner){
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public List<Result> getRowsColumns(String tableName, String rowKey_like,String cols[]) {
        HTableInterface table=null;
        Result result=null;
        List<Result> list=new ArrayList<Result>();
        try {
            table = htablePool.getTable(tableName);
            //rowkey的模糊匹配
            PrefixFilter filter=new PrefixFilter(rowKey_like.getBytes());
            Scan scan=new Scan();
            for(int i=0;i<cols.length;i++){
                scan.addColumn("cf".getBytes(),cols[i].getBytes());
            }
            scan.setFilter(filter);
            ResultScanner resultScanner=table.getScanner(scan);
            for(Result rs : resultScanner){
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public List<Result> getRows(String tableName, String startRow, String endRow) {
        HTableInterface table=null;
        List<Result> list=null;
        try {
            table = htablePool.getTable(tableName);
            Scan scan=new Scan();
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());
            ResultScanner scanner=table.getScanner(scan);
            list=new ArrayList<Result>();
            for(Result result : scanner){
                list.add(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static void main(String[] args) {
        HbaseDao hbaseDao=new HbaseDaoImpl();
//        Put put=new Put("10005".getBytes());
//        put.add("info".getBytes(),"name".getBytes(),"xijinping".getBytes());
//        put.add("info".getBytes(),"age".getBytes(),"80".getBytes());
//        put.add("info".getBytes(),"address".getBytes(),"beijing".getBytes());
//        hbaseDao.save(put,"user");
//        hbaseDao.insert("user","10006","info","name","zhaoliu");
        //       -------------------------------
        //名称name和age一样，会把以前的覆盖掉
//        List<Put> list=new ArrayList<Put>();
//        Put put=new Put("bbbb".getBytes());
//        put.add("info".getBytes(),"name".getBytes(),"ccc".getBytes());
//        put.add("info".getBytes(),"age".getBytes(),"ddd".getBytes());
//        list.add(put);
//        hbaseDao.save(list,"user");
        // ---------------------------------------
//        Result rs=hbaseDao.getOneRowResult("user","10002");
//        for(KeyValue keyValue : rs.raw()){
//            System.out.println("rowkey:"+new String(keyValue.getRow()));
//            System.out.println("qualify:"+new String(keyValue.getQualifier()));
//            System.out.println("value:"+new String(keyValue.getValue()));
//            System.out.println("-----------------------------");
//        }

//        List<Result> list=hbaseDao.getRows("user","10002");
//        for(Result rs : list){
//            for(KeyValue keyValue : rs.raw()){
//            System.out.println("rowkey:"+new String(keyValue.getRow()));
//            System.out.println("qualify:"+new String(keyValue.getQualifier()));
//            System.out.println("value:"+new String(keyValue.getValue()));
//            System.out.println("-----------------------------");
//        }
//        }
        List<Result> list=hbaseDao.getRowsColumns("hbase_state","2018-05-23",new String[]{"amt_1"});
        for(Result rs : list){
            for(KeyValue keyValue : rs.raw()){
                System.out.println("rowkey:"+new String(keyValue.getRow()));
                System.out.println("qualify:"+new String(keyValue.getQualifier()));
                System.out.println("value:"+new String(keyValue.getValue()));
                System.out.println("-----------------------------");
            }
        }
    }
}
