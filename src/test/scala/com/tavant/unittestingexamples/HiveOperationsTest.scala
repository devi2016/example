package com.tavant.unittestingexamples

import com.tavant.unittestingexamples.HiveOperations
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import java.io.File

class HiveOperationsTest extends FunSuite  with DataFrameSuiteBase {
System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
  test("simple test") {
    
    val path = "src/test/resources/emp.txt"
    val warehousePath = "D:\\holdenk_test\\wharehouse"
    /// val mataStorePath="C:\\wharehouse\\metastore"
    val tableName = "hivetest.emp1"
    val sql = "CREATE EXTERNAL TABLE IF NOT EXISTS hivetest.emp1(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION '/tmp/data/emp1'"
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val localWarehousePath = new File(warehousePath, "wharehouse").getCanonicalPath
    //val localMetastorePath = new File(mataStorePath, "metastore").getCanonicalPath
    //sqlCtx.sql(s"SET hive.metastore.uris=${localWarehousePath}")
    sqlCtx.sql(s"SET hive.metastore.warehouse.dir=${localWarehousePath}")
    sqlCtx.sql("create database IF NOT EXISTS hivetest")
    sqlCtx.sql("DROP TABLE IF EXISTS " + tableName)
    sqlCtx.sql("use hivetest")
    
    HiveOperations.createHiveTables(sqlCtx, sql)
    HiveOperations.LoadData(sqlCtx, path, tableName)
    val count = sqlCtx.sql("select * from emp1").count()
    sqlCtx.sql("select * from emp1").show()
    assert(count === 5)
  }
  test("test2") {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
    val path = "src/test/resources/emp.txt"
    val warehousePath = "D:\\holdenk_test\\wharehouse"
    /// val mataStorePath="C:\\wharehouse\\metastore"
    val tableName = "hivetest.emp1"
    val sql = "CREATE EXTERNAL TABLE IF NOT EXISTS hivetest.emp1(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION '/tmp/data/emp1'"
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    sqlCtx.sql("use hivetest")
    val result = HiveOperations.createHiveTables(sqlCtx, sql)

    //assert(result == true)

  }
}