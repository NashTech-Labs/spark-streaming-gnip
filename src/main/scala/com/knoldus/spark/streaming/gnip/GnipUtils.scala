package com.knoldus.spark.streaming.gnip

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{ JavaReceiverInputDStream, JavaDStream, JavaStreamingContext }
import org.apache.spark.streaming.dstream.{ ReceiverInputDStream, DStream }

object GnipUtils {

  /**
   * Create an input stream that returns tweets received from Gnip.
   * @param ssc          StreamingContext object
   * @param url          Gnip Powertrack Url
   * @param username     Gnip account username
   * @param password     Gnip account password
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
    ssc: StreamingContext,
    url: String,
    username: String,
    password: String,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2): ReceiverInputDStream[String] =
    new GnipInputDStream(ssc, url, username, password, storageLevel)

  /**
   * Create an input stream that returns tweets received from Gnip.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param url       Gnip Powertrack Url
   * @param username  Gnip account username
   * @param password  Gnip account password
   */
  def createStream(
    jssc: JavaStreamingContext,
    url: String,
    username: String,
    password: String): JavaReceiverInputDStream[String] = {
    createStream(jssc.ssc, url, username, password)
  }

  /**
   * Create an input stream that returns tweets received from Gnip.
   * @param jssc      JavaStreamingContext object
   * @param url       Gnip Powertrack Url
   * @param username  Gnip account username
   * @param password  Gnip account password
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream(
    jssc: JavaStreamingContext, url: String,
    username: String,
    password: String,
    storageLevel: StorageLevel): JavaReceiverInputDStream[String] = {
    createStream(jssc.ssc, url, username, password, storageLevel)
  }

}
