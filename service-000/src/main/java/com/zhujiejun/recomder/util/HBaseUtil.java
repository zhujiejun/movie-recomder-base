package com.zhujiejun.recomder.util;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@SuppressWarnings("unused")
public class HBaseUtil {
    private static Admin ADMIN;
    private static Connection CONNECTION;
    private static final Configuration CONFIG;

    //获取Admin对象
    static {
        CONFIG = HBaseConfiguration.create();
        CONFIG.set("hbase.zookeeper.quorum", "node101");
        CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            CONNECTION = ConnectionFactory.createConnection(CONFIG);
            ADMIN = CONNECTION.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //显示表
    private static void show(Cell cell) {
        log.info("-----------------------------------------------------------------------");
        log.info("----------columnFamily: {}----------", Bytes.toString(CellUtil.cloneFamily(cell)));
        log.info("----------rowKey: {}----------", Bytes.toString(CellUtil.cloneRow(cell)));
        log.info("----------column: {}----------", Bytes.toString(CellUtil.cloneQualifier(cell)));
        log.info("----------value: {}----------", Bytes.toString(CellUtil.cloneValue(cell)));
        log.info("----------timestamp: {}----------", cell.getTimestamp());
        log.info("-----------------------------------------------------------------------");
    }

    //是否存在
    public static boolean isTableExist(String tableName) throws Throwable {
        return ADMIN.tableExists(TableName.valueOf(tableName));
    }

    //创建表
    public static void createTable(String tableName, String... columnFamily) throws Throwable {
        if (isTableExist(tableName)) {
            log.info("----------table {} existed----------", tableName);
        } else {
            List<ColumnFamilyDescriptor> columnFamilies = Lists.newArrayList();
            for (String CF : columnFamily) {
                columnFamilies.add(ColumnFamilyDescriptorBuilder.of(CF));
            }
            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(columnFamilies).build();
            ADMIN.createTable(descriptor);
            log.info("----------table {} create success!----------", tableName);
        }
    }

    //删除表
    public static void dropTable(String tableName) throws Throwable {
        if (isTableExist(tableName)) {
            ADMIN.disableTable(TableName.valueOf(tableName));
            ADMIN.deleteTable(TableName.valueOf(tableName));
            log.info("----------table {} delete success!----------", tableName);
        } else {
            log.info("----------table {} not exist!----------", tableName);
        }
    }

    //向表中插入数据
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws Throwable {
        Table table = CONNECTION.getTable(TableName.valueOf(tableName));
        Put data = new Put(Bytes.toBytes(rowKey));
        data.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(data);
        table.close();
        log.info("----------add data success!----------");
    }

    //删除多行数据
    public static void deleteMultiRow(String tableName, String... rowKeys) throws Throwable {
        Table table = CONNECTION.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<>();
        for (String row : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
        log.info("----------delete data success!----------");
    }

    //获取所有数据
    public static void getAllRows(String tableName) throws Throwable {
        Scan scan = new Scan();
        Table table = CONNECTION.getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                show(cell);
            }
        }
    }

    //获取某一行数据
    public static void getRow(String tableName, String rowKey) throws Throwable {
        Table table = CONNECTION.getTable(TableName.valueOf(tableName));
        Get rowKeys = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(rowKeys);
        for (Cell cell : result.rawCells()) {
            show(cell);
        }
    }

    //获取某一行指定"列族:列"的数据
    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws Throwable {
        Table table = CONNECTION.getTable(TableName.valueOf(tableName));
        Get rowKeys = new Get(Bytes.toBytes(rowKey));
        rowKeys.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(rowKeys);
        for (Cell cell : result.rawCells()) {
            show(cell);
        }
    }
}
