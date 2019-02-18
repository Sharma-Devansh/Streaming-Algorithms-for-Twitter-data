import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.mutable.Map
import twitter4j.FilterQuery


object TwitterStreaming {

  var sampleTweets = new ListBuffer[Status]()
  var sumTweetLength = 0
  var hashtagsDict = Map[String, Int]()
  var Number = 0
  val Sample = 100

  def EachBatch(batches : RDD[Status]) : Unit = {

    val tweetCollection = batches.collect()
    //println(type(tweets))
    //println(tweets.length)
    var count1 = 0
    for(status <- tweetCollection ){
      //println(count1)
      if (Number >= Sample){
        val j = Random.nextInt(Number)
        if(j < Sample){
          val tweetToBeRemoved = sampleTweets(j)
          sampleTweets(j) = status
          sumTweetLength = sumTweetLength + status.getText().length - tweetToBeRemoved.getText().length

          // Delete old ones
          val hashTags = tweetToBeRemoved.getHashtagEntities().map(_.getText)
          for(tag <- hashTags){
            hashtagsDict(tag) -= 1
          }

          // Insert new ones
          val newHashTags = status.getHashtagEntities().map(_.getText)
          for(tag <- newHashTags){
            if(hashtagsDict.contains(tag)){
              hashtagsDict(tag) += 1
            }
            else{
              hashtagsDict(tag) = 1
            }
          }
          val topTags = hashtagsDict.toSeq.sortWith(_._2 > _._2)
          val size = topTags.size.min(5)
          println("The number of the twitter from beginning: " + (Number + 1))
          println("Top 5 hot hashtags:")
          for(i <- 0 until size){
            if(topTags(i)._2 != 0){
              println(topTags(i)._1 + ":" + topTags(i)._2)
            }
          }
          println("The average length of the twitter is: " +  sumTweetLength/(Sample.toFloat))
          println()
          println()
        }
      }
      else {
        sampleTweets.append(status)
        sumTweetLength = sumTweetLength + status.getText().length

        val hashTags = status.getHashtagEntities().map(_.getText)
        for(tag <- hashTags){
          if(hashtagsDict.contains(tag)){
            hashtagsDict(tag) += 1
          }
          else{
            hashtagsDict(tag) = 1
          }
        }
      }


      Number = Number + 1
      count1 += 1
    }
    //println(count1)
  }

  def main(args: Array[String]): Unit =
  {

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