package com.tavant.unittestingexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object HiveOperations {
  def main(args: Array[String]): Unit = {    
    init()      
  }
  def init()
    {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");
    val path = "src/test/resources/emp.txt"
    val tableName = "hivetest.emp1"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val sql="CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    createHiveTables(sqlContext,sql)
    LoadData(sqlContext,path,tableName)
    }
  def createHiveTables(sqlContext:HiveContext,sql:String):Boolean=
    {
      sqlContext.sql(sql)
      println("Testttttttttttttttttttttttttttttttttttttttttt")
      return true;
    }
   def LoadData(sqlContext:HiveContext,path:String,tableName:String)
   {
    sqlContext.sql("LOAD DATA LOCAL INPATH '" +path + " ' OVERWRITE INTO TABLE "+tableName);
   }
}