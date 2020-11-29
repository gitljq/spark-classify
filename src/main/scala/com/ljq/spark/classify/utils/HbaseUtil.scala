package com.ljq.spark.classify.utils

import java.io.IOException
import java.text.DecimalFormat
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}

/**
 * @Description:
 * @Author: ljq_dmr
 * @Date: 2020/11/29 17:13
 */
class HbaseUtil {
  val conf: Configuration = HBaseConfiguration.create
  conf.set("hbase.zookeeper.quorum","192.168.80.101")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("zookeeper.znode.parent", "/hbase")
  val connection:Connection = ConnectionFactory.createConnection(conf);

  def putData(tableName: String, id: String, familyName: String, columns: Array[String], values: Array[String]): Unit = {
    val rowKey: String = getRowKey(id)
    var table: Table = null
    try {
      table = getTable(tableName)
      val put = new Put(Bytes.toBytes(rowKey))
      if (columns != null && columns.length == values.length) for (i <- 0 until columns.length) {
        if (values(i) != null){
          put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columns(i)), Bytes.toBytes(values(i)))
        }
      }
      table.put(put)
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    } finally close(null, null, table)
  }

  def getRowData(tableName: String, id: String, family: String, columns: Array[String]): util.Map[String, String] = {
    val rowKey: String = getRowKey(id)
    //返回的键值对
    val result = new util.HashMap[String, String]
    //只查询指定列
    val get = new Get(Bytes.toBytes(rowKey))
    for (column <- columns) {
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
    }
    // 获取表
    var table: Table = null
    try {
      table = getTable(tableName)
      val hTableResult: Result = table.get(get)
      if (hTableResult != null && !hTableResult.isEmpty) {
        import scala.collection.JavaConversions._
        for (cell <- hTableResult.listCells) {
          result.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    } catch {
      case e: IOException =>
        throw new RuntimeException("查询一行的数据失败")
    } finally close(null, null, table)
    result
  }

  /**
   * 关闭流
   */
  private def close(admin: Admin, rs: ResultScanner, table: Table): Unit = {
    if (admin != null) try admin.close()
    catch {
      case e: IOException =>
        throw new RuntimeException("关闭Admin失败")
    }
    if (rs != null) rs.close()
    if (table != null) try table.close()
    catch {
      case e: IOException =>
        throw new RuntimeException("关闭Table失败")
    }
  }

  private def getTable(tableName: String) = connection.getTable(TableName.valueOf(tableName))

  /**
   * 根据id生成rowkey,这里已经确定regNum是96,
   *
   * @param id
   * @return
   */
  private def getRowKey(id: String) = {
    val hash: Int = (id.hashCode & Integer.MAX_VALUE) % 96
    val df = new DecimalFormat
    df.applyPattern("00")
    val regNo: String = df.format(hash)
    regNo + "_" + id
  }


  def createTable(namespace: String, tableName: String, familyName: String,regNum: Integer): Unit = {
    var admin: Admin = null
    try { // 获取HBase管理员对象
      admin = connection.getAdmin
      val tablename: TableName = TableName.valueOf(namespace, tableName)
      if (admin.tableExists(tablename)) {
        return
      }
      val splitKeys = new Array[Array[Byte]](regNum-1)
      for (i <- 0 until regNum - 1) {
        var format: String =String.format("%02d", Integer.valueOf(i + 1))
        splitKeys(i) = Bytes.toBytes(format)
      }
      //表描述器构造器
      val builder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tablename)
      //列族描述器构造器
      val cdb: ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName))
      //获得列描述器
      val cf: ColumnFamilyDescriptor = cdb.build
      cdb.setMaxVersions(1)
      //添加列族
      builder.setColumnFamily(cf)
      //获得表描述器
      val descriptor: TableDescriptor = builder.build
      admin.createTable(descriptor, splitKeys)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally close(admin, null, null)
  }
}
