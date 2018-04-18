package com.ibm.spark.streaming.mq

import java.net.ServerSocket
import java.util.{Properties, UUID}
import javax.jms._
import javax.naming.{Context, InitialContext}
import com.ibm.mq.jms._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Minutes, Seconds}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType}

import java.util.Calendar
import scala.concurrent.duration._

object SparkMQExample
{
    def functionToCreateContext(host: String, port: String, qm: String, qn: String, checkpointDirectory: String): StreamingContext = {
        val sparkConf = new SparkConf().setAppName("MQJMSReceiver").set("spark.streaming.receiver.writeAheadLog.enable", "true")
        val sc = new SparkContext(sparkConf)
        
        val ssc = new StreamingContext(sc, Seconds(10))
        
        ssc.checkpoint(checkpointDirectory)
        
        // Credentials were used here as we didn't wish to pass as a parameter; passing a file with credentials could be considered also
        val user = ""
        val credentials = ""
        
        val converter: Message => Option[String] = {
            case msg: TextMessage =>
                Some(msg.getText)
            case _ =>
                None
        }
        
        val msgs = JmsStreamUtils.createSynchronousJmsQueueStream(ssc, MQConsumerFactory(host, port.toInt, qm, qn, user, credentials),
                                                                 converter,
                                                                  1000,
                                                                  1.second,
                                                                  10.seconds,
                                                                  StorageLevel.MEMORY_AND_DISK_SER_2
                                                                 )
        
        msgs.foreachRDD ( rdd => {
            if (!rdd.partitions.isEmpty){
                // This is where you process the messages received
                println("messages received:")
                rdd.foreach(println)
                // You can save the collection of messages to HDFS (or alternatively locally by specifying "file:///")
                // The timestamp was added here to ensure unique folder ids
                rdd.saveAsTextFile("hdfs:///...-"+Calendar.getInstance().getTimeInMillis())
            } else {
                println("rdd is empty")
            }
        })
        
        ssc
    }
    
    def main(args: Array[String]): Unit = {
        if (args.length < 4){
            System.err.println("Usage: <host> <port> <queue manager> <queue name>")
            System.exit(1)
        }
        
        val Array(host, port, qm, qn) = args
        
        // Specify the path for the checkpoint directory can be local file or hdfs
        // This is used to recover messages if the application fails
        val checkpointDirectory = "hdfs:///..."
        
        // Get StreamingContext from checkpoint data or create a new one
        val streamC = StreamingContext.getOrCreate(checkpointDirectory, () => functionToCreateContext(host, port, qm, qn, checkpointDirectory))
        
        streamC.start()
        streamC.awaitTermination()
    }
}