package com.tuandai.kafka.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public interface HbaseDao {
    public void save(Put put, String tableName);
    public void insert(String tableName, String rowKey, String family, String quality, String value);

    public void save(List<Put> put, String tableName);
    public Result getOneRowResult(String tableName, String rowKey);
    public List<Result> getRows(String tableName, String rowKey_like);
    public List<Result> getRowsColumns(String tableName, String rowKey_like, String cols[]);
    public List<Result> getRows(String tableName, String startRow, String endRow);
}
