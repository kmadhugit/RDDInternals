package org.madhu

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.{Status, TwitterFactory}
import twitter4j.auth.AccessToken

/**
 * Spark Application that gets the word count of hash tags in our 10 second interval sample.
 * Created by thirumal on 21/9/15.
 */
object HelloTwitter {
  private var numTweetsCollected = 0L
  def main(args: Array[String]): Unit = {
    // Add your own Twitter API configuration stuff
    //////////////// TWITTER API CONF PART START ////////////////
    val consumerKey = "9QR5oQZYJyO2I8vCXlAwZHhtT"
    val consumerSecret = "wfj2TcfQpHioAiNPioYXCxbIk0cs7tX9IDl1EydCsuuT3MF3tz"
    val accessToken = "3124911061-OsRjd7sdQYRXQ2IxkTzjb9X5QIwd7dWLBJC56BZ"
    val accessTokenSecret = "5cGf655wG2Fr6UP96BWdwNgx7PF8hYXb8kkk6ETfSWSHb"
    //////////////// TWITTER API CONF PART END  ////////////////

    // Disable tons of logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Our usual spark context
    val sc = new SparkConf(false).setMaster("local[4]").setAppName("Hello Streaming").set("spark.logConf", "true")

    // Streaming context with 30 seconds batch size
    val ssc = new StreamingContext(sc, Seconds(5))

    // Get a twitter instance and then configure it with all our secret auth stuff.
    // finally get an auth object to be used with TwitterUtils
    val twitter = new TwitterFactory().getInstance()
    twitter.setOAuthConsumer(consumerKey, consumerSecret)
    twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))
    val authStuff = Option(twitter.getAuthorization)

    // Create the stream2
    val stream = TwitterUtils.createStream(ssc, authStuff, Array("BDA-2015"))

    // Get HashTags
    //val statuses = stream.filter(_.getLang == "en").map(_.getText).flatMap(_.split(" "))

    /* statuses.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if(count > 0) {
        rdd.coalesce(1).saveAsTextFile("/home/kmadhu/twitter_data/data" + time.milliseconds.toString)
        numTweetsCollected += count
        println("Number of tweets collected: " + numTweetsCollected)
        if (numTweetsCollected > 1000000) {
          System.exit(0)
        }
      }
    }) */


    def process(x : RDD[Status]) : Unit = {

      //val f = x.filter(s=> ( s.getText.contains("please") || s.getText.startsWith("plz") || s.getText.startsWith("pls"))   )
      val f = x

      f.foreach(y => {
        println(y.getUser().getName + "===>" + y.getText() + "\n\n")
      })
/*     f.foreach(y => {
       println(y.getUser().getName + "===>" + y.getText.map(a => if(a=='\n') ' ' else a ))
     })*/
    }
    stream.foreachRDD(process(_))
    //stream.foreachRDD((rdd, time) => { rdd.map(r => { r.getUser() } ).foreach(x=>println(x)) } )
    // println("Number of tweets collected: " + numTweetsCollected)
    ssc.start()
    ssc.awaitTermination()
  }
}
