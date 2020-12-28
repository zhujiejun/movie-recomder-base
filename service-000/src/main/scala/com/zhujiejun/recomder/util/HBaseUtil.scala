package com.zhujiejun.recomder.util

import com.google.common.collect.Lists
import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data.{Movie, Rating, Tag}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConversions._

@SuppressWarnings(Array("unused", "deprecation"))
object HBaseUtil {
    //获取Admin对象
    private val CONFIG: Configuration = HBaseConfiguration.create()
    CONFIG.set("hbase.zookeeper.quorum", "node101")
    CONFIG.set("hbase.zookeeper.property.clientPort", "2181")
    private val CONNECTION: Connection = ConnectionFactory.createConnection(CONFIG)
    private val ADMIN: Admin = CONNECTION.getAdmin
    private val log: Logger = LoggerFactory.getLogger("HBaseUtil")

    /*{
        CONFIG.set("hbase.zookeeper.quorum", "node101")
        CONFIG.set("hbase.zookeeper.property.clientPort", "2181")
    }*/

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
            //val list: java.util.List[Int] = List(1,2,3,4).asJava
            val columnFamilies: java.util.List[ColumnFamilyDescriptor] = Lists.newArrayList()
            for (sf <- columnFamily) {
                columnFamilies.add(ColumnFamilyDescriptorBuilder.of(sf))
                //columnFamilies :+ ColumnFamilyDescriptorBuilder.of(sf)
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

    //获取指定"列族:列"的数据Movie
    @throws[Throwable]
    def getMoviesFromHbase(tableName: String, family: String): List[Movie] = {
        val movies: List[Movie] = List()
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val resultScanner = table.getScanner(Bytes.toBytes(family))
        resultScanner.foreach { result =>
            val cells = result.rawCells()
            cells.foreach { cell => {
                var movieMap: Map[String, String] = Map()
                val column = Bytes.toString(CellUtil.cloneQualifier(cell))
                val value = Bytes.toString(CellUtil.cloneValue(cell))
                column match {
                    case "mid" => movieMap += ("mid" -> value)
                    case "name" => movieMap += ("name" -> value)
                    case "descri" => movieMap += ("descri" -> value)
                    case "timelong" => movieMap += ("timelong" -> value)
                    case "issue" => movieMap += ("issue" -> value)
                    case "shoot" => movieMap += ("shoot" -> value)
                    case "language" => movieMap += ("language" -> value)
                    case "genres" => movieMap += ("genres" -> value)
                    case "actors" => movieMap += ("actors" -> value)
                    case "directors" => movieMap += ("directors" -> value)
                    case _ => println
                }
                val movie = Movie(movieMap("mid").toInt, movieMap("name"), movieMap("descri"),
                    movieMap("timelong"), movieMap("issue"), movieMap("shoot"), movieMap("language"),
                    movieMap("genres"), movieMap("actors"), movieMap("directors"))
                movies.+:(movie)
            }
            }
        }
        movies
    }

    //获取指定"列族:列"的数据Rating
    @throws[Throwable]
    def getRatingsFromHbase(tableName: String, family: String): List[Rating] = {
        val ratings: List[Rating] = List()
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val resultScanner = table.getScanner(Bytes.toBytes(family))
        resultScanner.foreach { result =>
            val cells = result.rawCells()
            cells.foreach { cell => {
                var ratingMap: Map[String, String] = Map()
                val column = Bytes.toString(CellUtil.cloneQualifier(cell))
                val value = Bytes.toString(CellUtil.cloneValue(cell))
                column match {
                    case "uid" => ratingMap += ("uid" -> value)
                    case "mid" => ratingMap += ("mid" -> value)
                    case "score" => ratingMap += ("score" -> value)
                    case "timestamp" => ratingMap += ("timestamp" -> value)
                    case _ => println
                }
                val rating = Rating(ratingMap("uid").toInt, ratingMap("mid").toInt, ratingMap("score").toDouble, ratingMap("timestamp").toInt)
                ratings.+:(rating)
            }
            }
        }
        ratings
    }

    def checkTableExistInHabse(): Unit = {
        if (!HBaseUtil.isTableExist(HBASE_MOVIE_TABLE_NAME)) {
            println(s"----------the table $HBASE_MOVIE_TABLE_NAME  not existed, create the table----------")
            HBaseUtil.createTable(HBASE_MOVIE_TABLE_NAME, HBASE_MOVIE_COLUMN_FAMILY,
                HBASE_RATING_COLUMN_FAMILY, HBASE_TAG_COLUMN_FAMILY)
        }
    }

    //Movie
    def storeMovieDataInHabse(implicit data: RDD[Movie], save: RDD[Movie] => Unit): Unit = {
        save(data)
    }

    //Rating
    def storeRatingDataInHabse(implicit data: RDD[Rating], save: RDD[Rating] => Unit): Unit = {
        save(data)
    }

    //Tag
    def storeTagDataInHabse(implicit data: RDD[Tag], save: RDD[Tag] => Unit): Unit = {
        save(data)
    }

    def main(args: Array[String]): Unit = {
        //System.out.println(HBaseUtil.isTableExist("sfb_base"))
        HBaseUtil.getMoviesFromHbase(HBASE_MOVIE_TABLE_NAME, HBASE_MOVIE_COLUMN_FAMILY).foreach { movie =>
            println(s"---------the current movie is ${movie.toString}---------")
        }
    }
}
