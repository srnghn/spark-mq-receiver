# Spark MQ Receiver

For users that wish to connect use IBM Webshere MQ as a data source for Spark Streaming.

This package is an extension of JMS spark receiver: `https://github.com/tbfenet/spark-jms-receiver`

The code contains a reliable receiver for Spark streaming from an MQ source. Use `com.ibm.spark.streaming.mq.MQJmsStreamUtils` to create `InputDStream`.

The code still contains a reliable receiver for Spark streaming from any JMS 1.1 source. Use `net.tbfe.spark.streaming.jms.JmsStreamUtils` to create `InputDStream`.

## Build Procedure

Checkout the master branch `https://github.ibm.com/DataScienceCoC/spark-streaming-mq-connector-library.git`

### Install required MQ Jars

The required MQ jars cannot be installed by referring to them in *libraryDependencies* within the sbt build file. Therefore, they must be sourced manually and referred to in a different manner.

The [sbt Manual](http://www.scala-sbt.org/release/docs/Library-Dependencies.html) provides the options for these unmanaged dependencies.

Once you have sourced the libraries, as per the instructions below, update the sbt build file specifying their path.

#### WebSphere MQ Version 8.0 
**Download**
If you are connecting to a WebSphere MQ Version 8.0 instance, the WebSphere MQ Java self-extracting JAR can be downloaded from [Fix Central](https://www-945.ibm.com/support/fixcentral) To locate the latest file use the phrase "Java" in the Text Search box. Enure that "Show fixes that apply to this version" is selected under *Additional query options* This will find the latest available download. The name of the file to be downloaded will be in the format <V.R.M.F>-WS-MQ-Install-Java-All.jar

If you can't find the file, make sure that the Product Selected is WebSphere MQ and the Version is 8.0.

More thorough instructions can be found on the [IBM website](https://www-01.ibm.com/support/docview.wss?uid=swg21683398)

**Installation**
This jar file is executable. When run it will display the WebSphere MQ license agreement, which must be accepted. It will ask for a directory in which to install the WebSphere MQ classes for Java, classes for JMS, resource adapter and OSGi bundles. The directory will be created if it does not exist; if it already exists an error will be reported and no files will be installed.

To start the installation issue the command:
`java -jar <V.R.M.F>-WS-MQ-Install-Java-All.jar`

from the directory to which you downloaded the file, where `<V.R.M.F>-WS-MQ-Install-Java-All.jar` is the file that was downloaded from Fix Central. This requires a Java Runtime to have been installed on your machine and in the system path.

#### Older Versions
If you are connecting to a different version of MQ, you must install the full MQ Client. More information can be found [here](http://www-01.ibm.com/software/integration/wmq/clients/) and [here](http://www-01.ibm.com/support/docview.wss?uid=swg21376217)

### How to build Spark-MQ

Once you have amended the sbt build file as per instructions above:
- Change directory to `spark-mq`
- Execute `sbt package`

## Reliability Components

### SynchronousJmsReceiver
Only an example of SynchronousJmsReceiver is provided as during the development of this code, it was clear that reliability was required

### Checkpointing
To further ensure no loss of data, checkpointing is included within the example provided for MQ spark receiver

## Steps for connecting to MQ

1. Create an instance of MQConsumerFactory, passing required host name, port number, queue manager, queue name, user and credentials for the MQ instance you are connecting to.

2. Use the JmsStreamUtils method createSynchronousJmsQueueStream to create the Input DStream

An example is included within the test directory, it takes host name, port number, queue manager and queue name as parameters. There following edits should be made to work with your MQ instance:

1. Either update the values `user` and `credentials` for example, by hardcoding (as included in the example), or receiving these as a string parameter or receiving a credentials document.

2. In addition to printing the received messages to the console, this example writes the messages to HDFS. If you wish to use this functionality, you will need to specify a path (which can be to the local filesystem if you prefix with `file://` or HDFS with `hdfs://`) or comment this out.

3. This example includes checkpointing and therefore requires a path to save this checkpointing information. Again, this can be either a file on the local system or HDFS.

## Steps for using Checkpointing

1. Define a function that creates a Spark context, connects to MQ and processes the messages

2. Within main, call the StreamingContext method getOrCreate, passing the checkpoint location and the function you defined in step 1

