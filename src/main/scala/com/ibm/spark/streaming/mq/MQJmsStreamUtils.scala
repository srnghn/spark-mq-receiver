package com.ibm.spark.streaming.mq

import javax.jms._
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.jms._

import com.ibm.mq
import com.ibm.mq.jms._

/**
 *
 * @param host                      MQ hostname
 * @param port                      MQ port number
 * @param qmgr                      MQ queue manager name
 * @param qname                     MQ queue name / topic
 * @param uname                     User name for connecting to MQ Queue
 * @param creds                     Credentials for the specified user
 * @param connectionFactoryName     Name of connection factory configured in JNDI
 * @param messageSelector           Message selector. Use Empty string for no message filter
 */
case class MQConsumerFactory(mqHost: String,
                            mqPort: Int,
                            mqQmgr: String,
                            mqQname: String,
                            mqUser: String,
                            mqCred: String,
                            connectionFactoryName: String = "ConnectionFactory",
                            messageSelector: String = "")
extends MessageConsumerFactory with Logging {
    
    @volatile
    @transient
    var host: String = _
    var port: Int = _
    var qmgr: String = _
    var qname: String = _
    var uname: String = _
    var creds: String = _
    
    override def makeConsumer(session: Session): MessageConsumer = {
        var queue = new MQQueue()
        queue = session.createQueue(qname).asInstanceOf[MQQueue]
        session.createConsumer(queue, messageSelector)
    }
    
    override def makeConnection: Connection = {
        if (host == null){
            host = mqHost
            port = mqPort
            qmgr = mqQmgr
            qname = mqQname
            uname = mqUser
            creds = mqCred
        }
        val conFactory = new MQQueueConnectionFactory()
        conFactory.setHostName(host)
        conFactory.setPort(port)
        conFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP)
        conFactory.setQueueManager(qmgr)
        
        val qCon = conFactory.createQueueConnection(uname, creds).asInstanceOf[MQQueueConnection]
        
        qCon
    }
}

case class MQConsumerFactory(mqHost: String,
                            mqPort: Int,
                            mqQmgr: String,
                            mqQname: String,
                            mqUser: String,
                            mqCred: String,
                            mqChannel: String,
                            connectionFactoryName: String = "ConnectionFactory",
                            messageSelector: String = "")
extends MessageConsumerFactory with Logging {
    
    @volatile
    @transient
    var host: String = _
    var port: Int = _
    var qmgr: String = _
    var qname: String = _
    var uname: String = _
    var creds: String = _
    var channel: String = _
    
    override def makeConsumer(session: Session): MessageConsumer = {
        var queue = new MQQueue()
        queue = session.createQueue(qname).asInstanceOf[MQQueue]
        session.createConsumer(queue, messageSelector)
    }
    
    override def makeConnection: Connection = {
        if (host == null){
            host = mqHost
            port = mqPort
            qmgr = mqQmgr
            qname = mqQname
            uname = mqUser
            creds = mqCred
            channel = mqChannel
        }
        val conFactory = new MQQueueConnectionFactory()
        conFactory.setHostName(host)
        conFactory.setPort(port)
        conFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP)
        conFactory.setQueueManager(qmgr)
        conFactory.setChannel(channel)
        
        val qCon = conFactory.createQueueConnection(uname, creds).asInstanceOf[MQQueueConnection]
        
        qCon
    }
}
