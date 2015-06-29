# Spark-Streaming-Gnip
========================

An Apache Spark utility for pulling Tweets from Gnip's PowerTrack in realtime.

-----------------------------------------------------------------------
### Discription
-----------------------------------------------------------------------

This utility will help to pull tweets from Gnip using Spark Streaming. The Gnip, Inc. provides data from dozens of social media websites via a single API. It is also known as the Grand Central Station for social media web.

This code have implemented a Custom Receiver which uses Spark Streaming to fetch tweets from Gnip and 'store' it in Spark BlockManager.

The logic will automatically store tweets from Gnip for the interval of time specified in Spark Streaming Context.

Please see Scala code example on how to use this Spark Streaming Gnip utility.

-----------------------------------------------------------------------
### Scala Example
-----------------------------------------------------------------------

	import org.apache.spark.SparkConf
	import org.apache.spark.SparkContext
	import org.apache.spark.streaming.Seconds
	import org.apache.spark.streaming.StreamingContext
	import com.knoldus.spark.streaming.gnip.GnipUtils
 
	object GnipStreaming extends App {
 
 		val sparkConf: SparkConf = new SparkConf().setAppName("spark-streaming-gnip").setMaster("local[*]")
 		val sc: SparkContext = new SparkContext(sparkConf)
 		val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
 		val gnipDstream = GnipUtils.createStream(ssc, "https://stream.gnip.com:443/accounts/Knoldus/publishers/twitter/streams/track/prod.json", "knoldus@knoldus.com", "knoldus")
 
 		gnipDstream.print()
 
 		ssc.start()
 		ssc.awaitTermination()
 
	}

