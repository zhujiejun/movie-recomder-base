package com.zhujiejun.recomder.util

import com.google.common.collect.Lists
import com.zhujiejun.recomder.cons.Const._
import com.zhujiejun.recomder.data._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.jblas.DoubleMatrix
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

@SuppressWarnings(Array("unused", "deprecation"))
object HBaseUtil {
    //获取Admin对象
    private val CONFIG: Configuration = HBaseConfiguration.create()
    CONFIG.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
    CONFIG.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
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
        val movies: ListBuffer[Movie] = new ListBuffer[Movie]
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val resultScanner = table.getScanner(Bytes.toBytes(family))
        resultScanner.foreach { result =>
            var movieMap: Map[String, String] = Map()
            result.rawCells().foreach { cell => {
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
                    case _ => ""
                }
            }
            }
            val movie = Movie(movieMap("mid").toInt, movieMap("name"), movieMap("descri"),
                movieMap("timelong"), movieMap("issue"), movieMap("shoot"), movieMap("language"),
                movieMap("genres"), movieMap("actors"), movieMap("directors"))
            movies.append(movie)
        }
        movies.toList
    }

    //获取指定"列族:列"的数据Rating
    @throws[Throwable]
    def getRatingsFromHbase(tableName: String, family: String): List[Rating] = {
        val ratings: ListBuffer[Rating] = new ListBuffer[Rating]
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val resultScanner = table.getScanner(Bytes.toBytes(family))
        resultScanner.foreach { result =>
            var ratingMap: Map[String, String] = Map()
            result.rawCells().foreach { cell => {
                val column = Bytes.toString(CellUtil.cloneQualifier(cell))
                val value = Bytes.toString(CellUtil.cloneValue(cell))
                column match {
                    case "uid" => ratingMap += ("uid" -> value)
                    case "mid" => ratingMap += ("mid" -> value)
                    case "score" => ratingMap += ("score" -> value)
                    case "timestamp" => ratingMap += ("timestamp" -> value)
                    case _ => ""
                }
            }
            }
            val rating = Rating(ratingMap("uid").toDouble.toInt, ratingMap("mid").toDouble.toInt,
                ratingMap("score").toDouble, ratingMap("timestamp").toDouble.toInt)
            ratings.append(rating)
        }
        ratings.toList
    }

    @throws[Throwable]
    def getMovieRecsFromHbase(tableName: String, family: String): List[MovieRecs] = {
        val movieRecses: ListBuffer[MovieRecs] = new ListBuffer[MovieRecs]
        val table = CONNECTION.getTable(TableName.valueOf(tableName))
        val resultScanner = table.getScanner(Bytes.toBytes(family))
        resultScanner.foreach { result =>
            var movieRecsMap: Map[String, String] = Map()
            result.rawCells().foreach { cell => {
                val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
                movieRecsMap += ("mid0" -> rowKey)
                val mid1 = Bytes.toString(CellUtil.cloneQualifier(cell))
                val score = Bytes.toString(CellUtil.cloneValue(cell))
                movieRecsMap += (mid1 -> score)
            }
            }
            val mid0 = movieRecsMap("mid0").toDouble.toInt
            val recomders = for ((k, v) <- movieRecsMap) yield Recommendation(k.toInt, v.toDouble)
            val movieRecs = MovieRecs(mid0, recomders.toSeq)
            movieRecses.append(movieRecs)
        }
        movieRecses.toList
    }

    def checkTableExistInHabse(tableName: String): Unit = {
        tableName match {
            case ORIGINAL_MOVIE_TABLE_NAME =>
                if (!HBaseUtil.isTableExist(ORIGINAL_MOVIE_TABLE_NAME)) {
                    println(s"----------the table $ORIGINAL_MOVIE_TABLE_NAME  not existed, create the table----------")
                    HBaseUtil.createTable(ORIGINAL_MOVIE_TABLE_NAME, ORIGINAL_MOVIE_COLUMN_FAMILY,
                        ORIGINAL_RATING_COLUMN_FAMILY, ORIGINAL_TAG_COLUMN_FAMILY)
                }
            case STATIC_MOVIE_TABLE_NAME =>
                if (!HBaseUtil.isTableExist(STATIC_MOVIE_TABLE_NAME)) {
                    println(s"----------the table $STATIC_MOVIE_TABLE_NAME  not existed, create the table----------")
                    HBaseUtil.createTable(STATIC_MOVIE_TABLE_NAME, RATE_MORE_MOVIES_COLUMN_FAMILY,
                        RATE_MORE_RECENTLY_MOVIES_COLUMN_FAMILY, AVERAGE_MOVIES_COLUMN_FAMILY,
                        GENRES_TOP_MOVIES_COLUMN_FAMILY)
                }
            case OFFLINE_MOVIE_TABLE_NAME =>
                if (!HBaseUtil.isTableExist(OFFLINE_MOVIE_TABLE_NAME)) {
                    println(s"----------the table $OFFLINE_MOVIE_TABLE_NAME  not existed, create the table----------")
                    HBaseUtil.createTable(OFFLINE_MOVIE_TABLE_NAME, OFFLINE_USER_RECS_COLUMN_FAMILY,
                        MOVIE_FEATURES_RECS_COLUMN_FAMILY, MOVIE_CONTENTS_RECS_COLUMN_FAMILY)
                }
            case _ => println
        }
    }

    //求向量余弦相似度
    def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
        movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
    }

    def toYearMonth(timestamp: Long): String = {
        val instant = Instant.ofEpochMilli(timestamp)
        val localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault)
        localDateTime.format(DateTimeFormatter.ofPattern(YEAR_MONTH_PATTERN))
    }

    def main(args: Array[String]): Unit = {
        //val yearmonth = calYearMonth(System.currentTimeMillis())
        val yearmonth = toYearMonth(1609321726)
        println(yearmonth)
    }
}
