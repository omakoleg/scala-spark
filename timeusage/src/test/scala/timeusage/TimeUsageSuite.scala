package timeusage

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {



  // Assume only more than 2 columns always there. Not failsafe
  test("dfShema should work properly") {
    val columnNames = List("one", "two", "three")
    val results = TimeUsage.dfSchema(columnNames)
    val expected = StructType(List(
      StructField("one", StringType, false),
      StructField("two", DoubleType, false),
      StructField("three", DoubleType, false)
    ))
    assert(results == expected)
  }

  test("row") {
    assert(TimeUsage.row(List("a", "2", "3 ")) == Row("a", 2.0, 3.0))
  }

  //    val group1 = Seq("t01", "t03", "t11", "t1801", "t1803")
  //    val group2 = Seq("t05", "t1805")
  //    val group3 = Seq("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14","t15", "t16","t18")

  test("classifiedColumns separate data") {
    import TimeUsage.spark.implicits._
    assert(TimeUsage.classifiedColumns(List("t01aaa", "t05bbb", "t07ccc")) == (List($"t01aaa"), List($"t05bbb"), List($"t07ccc")))
  }

  test("classifiedColumns do not pass thru same args") {
    import TimeUsage.spark.implicits._
    assert(TimeUsage.classifiedColumns(List("t1805", "t1806", "t1803", "useless")) == (List($"t1803"), List($"t1805"), List($"t1806")))
  }
}
