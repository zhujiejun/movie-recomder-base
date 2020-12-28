package com.zhujiejun.recomder.util

import com.google.common.collect.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}

import java.util

@SuppressWarnings(Array("unused"))
object HBaseUtil {
    private var ADMIN: Admin = null
    private var CONNECTION: Connection = null
    private var CONFIG: Configuration = null
    private val log: Logger = LoggerFactory.getLogger("HBaseUtil")

    {
        //获取Admin对象
        CONFIG = HBaseConfiguration.create()
        CONFIG.set("hbase.zookeeper.quorum", "node101")
        CONFIG.set("hbase.zookeeper.property.clientPort", "2181")
        CONNECTION = ConnectionFactory.createConnection(CONFIG)
        ADMIN = CONNECTION.getAdmin
    }

    //显示表
    private def show(cell: Cell): Unit = {
        log.info("-----------------------------------------------------------------------")
        log.info("----------columnFamily: {}----------", Bytes.toString(CellUtil.cloneFamily(cell)))
        log.info("----------rowKey: {}----------", Bytes.toString(CellUtil.cloneRow(cell)))
        log.info("----------column: {}----------", Bytes.toString(CellUtil.cloneQualifier(cell)))
        log.info("----------value: {}----------", Bytes.toString(CellUtil.cloneValue(cell)))
        log.info("----------timestamp: {}----------", cell.getTimestamp)
        log.info("-----------------------------------------------------------------------")
    }

    //是否存在
    @throws[Throwable]
    def isTableExist(tableName: String): Boolean = ADMIN.tableExists(TableName.valueOf(tableName))

    //创建表
    @throws[Throwable]
    def createTable(tableName: String, columnFamily: String*): Unit = {
        if (isTableExist(tableName)) log.info("----------table {} existed----------", tableName)
        else {
            val columnFamilies: util.List[ColumnFamilyDescriptor] = Lists.newArrayList
            for (sf <- columnFamily) {
                columnFamilies.add(ColumnFamilyDescriptorBuilder.of(sf))
            }
            val descriptor: TableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamilies(columnFamilies).build
            ADMIN.createTable(descriptor)
            log.info("----------table {} create success!----------", tableName)
        }
    }

    //删除表
    @throws[Throwable]
    def dropTable(tableName: String): Unit = {
        if (isTableExist(tableName)) {
            ADMIN.disableTable(TableName.valueOf(tableName))
            ADMIN.deleteTable(TableName.valueOf(tableName))
            log.info("----------table {} delete success!----------", tableName)
        }
        else log.info("----------table {} not exist!----------", tableName)
    }

    //向表中插入数据
    @throws[Throwable]
    def addRowData(tableName: String, rowKey: String, columnFamily: String, column: String, value: String): Unit = {
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val data = new Put(Bytes.toBytes(rowKey))
        data.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
        table.put(data)
        table.close()
        log.info("----------add data success!----------")
    }

    //删除多行数据
    @throws[Throwable]
    def deleteMultiRow(tableName: String, rowKeys: String*): Unit = {
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val deleteList = new util.ArrayList[Delete]
        for (row <- rowKeys) {
            val delete = new Delete(Bytes.toBytes(row))
            deleteList.add(delete)
        }
        table.delete(deleteList)
        table.close()
        log.info("----------delete data success!----------")
    }

    //获取所有数据
    @throws[Throwable]
    def getAllRows(tableName: String): Unit = {
        val scan = new Scan
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val resultScanner = table.getScanner(scan)
        import scala.collection.JavaConversions._
        for (result <- resultScanner) {
            val cells = result.rawCells
            for (cell <- cells) {
                show(cell)
            }
        }
    }

    //获取某一行数据
    @throws[Throwable]
    def getRow(tableName: String, rowKey: String): Unit = {
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val rowKeys = new Get(Bytes.toBytes(rowKey))
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        val result = table.get(rowKeys)
        for (cell <- result.rawCells) {
            show(cell)
        }
    }

    //获取某一行指定"列族:列"的数据
    @throws[Throwable]
    def getRowQualifier(tableName: String, rowKey: String, family: String, qualifier: String): Unit = {
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val rowKeys = new Get(Bytes.toBytes(rowKey))
        rowKeys.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier))
        val result = table.get(rowKeys)
        for (cell <- result.rawCells) {
            show(cell)
        }
    }
}
