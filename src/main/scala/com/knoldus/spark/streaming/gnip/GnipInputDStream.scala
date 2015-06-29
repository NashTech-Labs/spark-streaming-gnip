package com.knoldus.spark.streaming.gnip

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.util.zip.GZIPInputStream

import scala.annotation.tailrec

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import sun.misc.BASE64Encoder

/* A stream of Gnip tweets.
 *
 * @constructor create a new Gnip connection using the supplied Gnip authentication credentials.
 * The Gnip API is such that this will return a set of all tweets during each interval.
 *
 */
class GnipInputDStream(
    @transient ssc_ : StreamingContext,
    url: String,
    username: String,
    password: String,
    storageLevel: StorageLevel) extends ReceiverInputDStream[String](ssc_) {

  override def getReceiver(): Receiver[String] = {
    new GnipReceiver(url, username, password, storageLevel)
  }
}

class GnipReceiver(
    url: String,
    username: String,
    password: String,
    storageLevel: StorageLevel) extends Receiver[String](storageLevel) with Logging {

  @volatile private var gnipConnection: HttpURLConnection = _
  @volatile private var stopped = false

  def onStart() {
    try {
      val newGnipConnection = getConnection(url, username, password)
      val inputStream = newGnipConnection.getInputStream()
      val responseCode = newGnipConnection.getResponseCode()
      if (responseCode >= 200 && responseCode <= 299) {
        val reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream), "UTF-8"))
        storeData(reader)
      } else {
        logError("Error receiving tweets")
      }
      setGnipConnection(newGnipConnection)
      logInfo("Gnip receiver started")
      stopped = false
    } catch {
      case e: Exception => restart("Error starting Gnip stream", e)
    }
  }

  def onStop() {
    stopped = true
    setGnipConnection(null)
    logInfo("Gnip receiver stopped")
  }

  private def getConnection(urlString: String, username: String, password: String): HttpURLConnection = {
    logInfo("Creating GNIP PowerTrack connection")
    val url = new URL(urlString)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setReadTimeout(1000 * 60 * 60)
    connection.setConnectTimeout(1000 * 20)
    connection.setRequestProperty("Authorization", createAuthHeader(username, password))
    connection.setRequestProperty("Accept-Encoding", "gzip")
    connection
  }

  private def createAuthHeader(username: String, password: String): String = {
    logWarning("Creating GNIP Authorization Headers")
    val encoder = new BASE64Encoder()
    val authToken = username + ":" + password
    "Basic " + encoder.encode(authToken.getBytes())
  }

  private def storeData(reader: BufferedReader): Boolean = {
    @tailrec
    def storeDataRecursively(reader: BufferedReader, line: String): Boolean = {
      if (line != null) {
        if (line != "") {
          store(line)
          storeDataRecursively(reader, reader.readLine())
        } else {
          storeDataRecursively(reader, reader.readLine())
        }
      } else {
        gnipConnection.disconnect()
        false
      }
    }
    storeDataRecursively(reader, reader.readLine())
  }

  private def setGnipConnection(newGnipConnection: HttpURLConnection) = synchronized {
    if (gnipConnection != null) {
      gnipConnection.disconnect()
    }
    gnipConnection = newGnipConnection
  }

}
