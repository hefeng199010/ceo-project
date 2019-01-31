package com.tuandai.ceo.utils;

import com.tuandai.tools.HBaseUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  只返回value 为Double 类型。
 */
public class CommonDoubleMap {
    //从Hbase 拿出数据放入map
    //如果要返回其他类型。直接调用HBaseUtils.getRowsByColumns自己做处理
    public static ConcurrentHashMap<String, Double> initMap(String table_name, String row_key, String[] ret) {
        ConcurrentHashMap<String, Double> map = new ConcurrentHashMap<String, Double>();
        List<Result> list = HBaseUtils.getRowsByColumns(table_name, row_key, "cf", ret);
        for (Result rs : list) {
            String rowkey = new String(rs.getRow());
            for (KeyValue keyValue : rs.raw()) {
                if (ret.equals(new String(keyValue.getQualifier()))) {
                    map.put(rowkey, Double.parseDouble(new String(keyValue.getValue())));
                }
            }
        }
        return map;
    }
//public static <T>Map<String, T> initMap(String table_name, String row_key, String ret) {
//
//    Map<String, T> map = new HashMap<String, T>();
//    List<Result> list = HBaseUtils.getRowsByColumns(table_name, row_key, "cf", new String[]{ret});
//    for (Result rs : list) {
//        String rowkey = new String(rs.getRow());
//        for (KeyValue keyValue : rs.raw()) {
//            if (ret.equals(new String(keyValue.getQualifier()))) {
//                    map.put(rowkey, (T)keyValue.getValue());
//            }
//        }
//    }
//    return map;
//}
    //map value = Double 数据累加，解决精度
    public static Map<String, Double> sumValue(Map<String, Double> map, String key, Double value) {
        if (map.get(key) == null) {
            map.put(key, value);
        } else {
//            value += map.get(key);
            map.put(key, DoubleOperationUtil.add(value, map.get(key)));
        }
        return map;
    }
    public static Map<String, String> sumValueAll(Map<String, Double> map, String key, Double value) {
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        Map<String,String> total_amt=new HashMap<>();
        nf.setGroupingUsed(false);
        if (map.get(key) == null) {
            map.put(key,value);
            total_amt.put(key, nf.format(value));
        } else {
//            value += map.get(key);
            map.put(key,DoubleOperationUtil.add(value, Double.valueOf(map.get(key))));
            total_amt.put(key, nf.format(DoubleOperationUtil.add(value, Double.valueOf(map.get(key)))));
        }
        return total_amt;
    }

}
