package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class SpecificTestsSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow

  val testConf = new SparkConf().setMaster("local").setAppName("StackOverflowTest")
  val sc: SparkContext = new SparkContext(testConf)

  test("Grouping questions and answers together") {
    val postings = List(
      Posting(1, 1, None, None, 0, None),
      Posting(1, 2, None, None, 0, None),
      Posting(2, 3, None, Some(1), 2, None),
      Posting(2, 4, None, Some(1), 5, None),
      Posting(2, 5, None, Some(2), 12, None),
      Posting(1, 6, None, None, 0, None)
    )
    val rdd = sc.makeRDD(postings)
    val results = testObject.groupedPostings(rdd).collect()

    assert(results.size === 2)
    assert(results.contains(
      (1, Iterable(
        (Posting(1, 1, None, None, 0, None), Posting(2, 3, None, Some(1), 2, None)),
        (Posting(1, 1, None, None, 0, None), Posting(2, 4, None, Some(1), 5, None))
      ))
    ))
    assert(results.contains(
      (2, Iterable(
        (Posting(1, 2, None, None, 0, None), Posting(2, 5, None, Some(2), 12, None))
      ))
    ))
  }

  test("Maximum scores among answers per question") {
    val groupedQuestions = Seq(
      (1, Iterable(
        (Posting(1, 1, None, None, 0, None), Posting(2, 3, None, Some(1), 2, None)),
        (Posting(1, 1, None, None, 0, None), Posting(2, 4, None, Some(1), 5, None))
      )),
      (2, Iterable(
        (Posting(1, 2, None, None, 0, None), Posting(2, 5, None, Some(2), 12, None))
      )),
      (3, Iterable(
        (Posting(1, 6, None, None, 0, None), Posting(2, 7, None, Some(3), 2, None)),
        (Posting(1, 6, None, None, 0, None), Posting(2, 8, None, Some(3), 19, None)),
        (Posting(1, 6, None, None, 0, None), Posting(2, 9, None, Some(3), 10, None))
      ))
    )
    val rdd = sc.makeRDD(groupedQuestions)
    val results = testObject.scoredPostings(rdd).collect()

    assert(results.size == 3)
    assert(results.contains( (Posting(1, 1, None, None, 0, None), 5) ))
    assert(results.contains( (Posting(1, 2, None, None, 0, None), 12) ))
    assert(results.contains( (Posting(1, 6, None, None, 0, None), 19) ))
  }

  test("Vectors for clustering") {
    val questionsWithTopAnswer = List(
      (Posting(1, 1, None, None, 0, Some("Java")), 14),
      (Posting(1, 1, None, None, 0, None), 5),
      (Posting(1, 1, None, None, 0, Some("Scala")), 25),
      (Posting(1, 1, None, None, 0, Some("JavaScript")), 3)
    )

    val rdd = sc.makeRDD(questionsWithTopAnswer)

    val result = testObject.vectorPostings(rdd).collect()
    assert (result === Array((50000, 14), (500000, 25), (0, 3)))
  }

  test("get median") {
    val sampleOne = Seq(
      (5000,2),
      (6000,3),
      (7000,1)
    )
    assert(testObject.getMedian(sampleOne) == 2)

    val sampleTwo = Seq(
      (5000,2),
      (6000,3),
      (6000,5),
      (7000,1)
    )
    assert(testObject.getMedian(sampleTwo) == 2)
  }

  test("from coursera debug") {
//    (Groovy,31,List(13, 13, 15, 16, 23, 26, 27, 76))
//    (Groovy,100.0,8,31)

    val sampleOne = Seq(
      (1,13),
      (1,13),
      (1,15),
      (1,16),
      (1,23),
      (1,26),
      (1,27),
      (1,76)
    )
    assert(testObject.getMedian(sampleOne) == 19)
  }

}
