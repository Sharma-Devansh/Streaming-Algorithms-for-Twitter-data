import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

import scala.Array.ofDim
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import Array._


object BloomFiltering {

  var N = 0
  val S = 100
  var hashtagsDict = Map[String, Int]()
  var set1: Set[String] = Set()
  var incorrect_list = new ListBuffer[String]
  var bloom_size = 235

  var bloom_array = ofDim[Int](bloom_size)

  def string_ascii(a: String, Md: Int): Int = {

    var sum = 0
    //var char = 0
    var char_arr = a.toCharArray()


    for(char <- char_arr)
    {
      sum = sum + char.toLower.toInt
      //  println(i.toLower.toInt)
    }

    return (sum % Md)
  }

  def string_fold(s: String, M: Int): Long = {
    val intLength: Int = s.length / 4
    var sum: Long = 0
    for (j <- 0 until intLength) {
      val c: Array[Char] = s.substring(j * 4, (j * 4) + 4).toCharArray()
      var mult: Long = 1
      for (k <- 0 until c.length) {
        sum += c(k) * mult
        mult *= 256
      }
    }
    val c: Array[Char] = s.substring(intLength * 4).toCharArray()
    var mult: Long = 1
    for (k <- 0 until c.length) {
      sum += c(k) * mult
      mult *= 256
    }
    return (Math.abs(sum) % M)
  }

  def mark_index(index : Integer) : Unit = {

    bloom_array(index) = 1

  }

  var incorrect_total = 0
  var correct_total = 0

  def EachBatch(batches : RDD[Status]) : Unit = {

    println("---------- 10 Seconds Batch Processing Starts ----------")
    val tweetCollection = batches.collect()
    var cur_batch_correct_list = new ListBuffer[String]
    var cur_batch_incorrect_list = new ListBuffer[String]
    var cur_batch_correct = 0
    var cur_batch_incorrect = 0

    //var set_contain = false
    //print("Tweets len: ")
    //println(tweets.length)
    for (status <- tweetCollection) {

      if (N < S) {
        val hashTags = status.getHashtagEntities().map(_.getText)

        for (hashtag <- hashTags) {
          //println("hashtag is -->",hashtag)

          var index1 = string_ascii(hashtag, bloom_size)
          var index2 = string_fold(hashtag, bloom_size).toInt

          var first_check = bloom_array(index1)
          var check_2 = bloom_array(index2)

          var str = set1.contains(hashtag)

          if (str) {
            cur_batch_correct_list.append(hashtag)
            cur_batch_correct += 1
            correct_total += 1
          }
          else {
            set1 = set1 + hashtag

            if (first_check == 1 && (check_2 == 1)) {
              incorrect_list.append(hashtag)
              cur_batch_incorrect_list.append(hashtag)
              incorrect_total += 1
              cur_batch_incorrect += 1
            }
            else {
              correct_total += 1
              cur_batch_correct += 1
              mark_index(index1)
              mark_index(index2)
              cur_batch_correct_list.append(hashtag)



            }
          }

        }
      }
    }
    print("Current Batch Correct Count: ")
    println(cur_batch_correct)
    print("Total Correct Count: ")
    println(correct_total)
    print("Current Batch Incorrect Count: ")
    println(cur_batch_incorrect)
    print("Total False positive count from starting: ")
    println(incorrect_total)

    println()
    print("Current Batch Correct hashtags are: ")
    for(x <- cur_batch_correct_list)
    {
      print(x + "  " )
    }
    println()

    print("Current Batch Incorrect Hashtags are: ")
    for(x <- cur_batch_incorrect_list)
    {
      print(x + "  ")
    }
    println()

    print("False Positives from beginning are: ")
    for(x <- incorrect_list)
    {
      print(x + "  " )
    }
    println()

    println("---------- Current 10 Seconds Batch Processing Ends ----------")
    println()
    println()


    return
  }

  def main(args: Array[String]): Unit = {

    val sConf = new SparkConf()
    sConf.setAppName("TwitterStreaming")
    sConf.setMaster("local[*]")
    sConf.set("spark.driver.host", "localhost")
    val sCon = new SparkContext(sConf)
    sCon.setLogLevel(logLevel = "OFF")

    val consumer_key = "8b9BRLhN0ZzlTYyig0u7NPfQd"
    val consumer_secret = "NQuoxTDozPM8wArz5cBtEmbfPTTAoZehov3FWRjKQ6uuG1pYay"
    val access_token = "955128865185124352-CZ2hwhkPpyXiM4de504768b0Oz1CXJa"
    val access_token_secret = "wNtAuTUmy5RRTCrP1teIvvt4Op4J4RraqSNvJQPyAsAP3"


    System.setProperty("twitter4j.oauth.consumerKey", consumer_key)
    System.setProperty("twitter4j.oauth.consumerSecret", consumer_secret)
    System.setProperty("twitter4j.oauth.accessToken", access_token)
    System.setProperty("twitter4j.oauth.accessTokenSecret", access_token_secret)

    val stCon = new StreamingContext(sCon, Seconds(10))
    val batches = TwitterUtils.createStream(stCon, None, Array("Data"))
    //print("tweets len : ")
    batches.foreachRDD(Batch => EachBatch(Batch))
    stCon.start()
    stCon.awaitTermination()

  }
}