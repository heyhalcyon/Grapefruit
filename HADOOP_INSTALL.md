## Supported Platform
* GNU/Linux is supported as a development and production platform. Hadoop has been demonstrated on GNU/Linux clusters with 2000 nodes.

* Windows is also a supported platform, but this guide only demonstrates how to install Hadoop on Linux.

## Required Software

Required software for Linux include:

* **Java** must be installed (Java 7 is recommended for newer versions).

* **ssh** must be installed and sshd must be running to use the Hadoop scripts that manage remote Hadoop daemons if the optional start and stop scripts are to be used.

* **pdsh** is also recommended to be installed for better ssh resource management.
	
	* run: `sudo yum install pdsh` on UIUC VM (OS: RHEL).

## Download Hadoop

* Download Hadoop source package: ` wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.7/hadoop-2.7.7.tar.gz`

* Unpack the installed package: `tar -xvf hadoop-2.7.7.tar.gz`

* Change directory: `cd hadoop-2.7.7/`

## Hadoop Cluster Configuration

1. Update `~/.bashrc` by adding the following lines:

```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.272.b10-1.el7_9.x86_64/jre
export PATH=$PATH:$JAVA_HOME/bin
export HADOOP_HOME=$HOME/hadoop-2.7.7
export HADOOP_CONF_DIR=$HOME/hadoop-2.7.7/etc/hadoop
export HADOOP_MAPRED_HOME=$HOME/hadoop-2.7.7
export HADOOP_COMMON_HOME=$HOME/hadoop-2.7.7
export HADOOP_HDFS_HOME=$HOME/hadoop-2.7.7
export YARN_HOME=$HOME/hadoop-2.7.7
export PATH=$PATH:$HOME/hadoop-2.7.7/bin
export HADOOP_CLASSPATH=/usr/lib/jvm/java-openjdk/lib/tools.jar
```

---

Also update `JAVA_HOME` path in the file: `$HOME/hadoop-2.7.7/etc/hadoop/hadoop-env.sh` as follows.

```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.272.b10-1.el7_9.x86_64/jre
export PATH=$PATH:$JAVA_HOME/bin
```

2. Run `source ~/.bashrc` command

3. Copy the master node's ssh key to slave's authorized keys.

`ssh-copy-id -i $HOME/.ssh/id_rsa.pub username@slave`

4. On the **master** node, create a file named `masters` under path: `$HOME/hadoop-2.7.7/etc/hadoop`

5. Add master node's public IP address to the file just created.

6. Add the IPs of all slaves to the file at path: `$HOME/hadoop-2.7.7/etc/hadoop/slaves` on the **master** node only.

7. Update `$HOME/hadoop-2.7.7/etc/hadoop/core-site.xml` on **all** nodes in the cluster with the following lines:

```xml
<configuration>
	<property>
		<name>fs.default.name</name>
		<value>hdfs://<MASTER_IP_ADDR>:9000</value>
	</property>
</configuration>
```

8. Update the file at path: `$HOME/hadoop-2.7.7/etc/hadoop/hdfs-site.xml` on the master node as follows:

```xml
<configuration>
	<property>
		<name>dfs.replication</name>
		<value>3</value>
	</property>
	<property>
		<name>dfs.permissions</name>
		<value>false</value>
	</property>
	<property>
		<name>dfs.namenode.name.dir</name>
		<value>/home/yitanze2/hadoop-2.7.7/namenode</value>
	</property>
	<property>
		<name>dfs.datanode.data.dir</name>
		<value>/home/yitanze2/hadoop-2.7.7/datanode</value>
	</property>
</configuration>
```

9. Update the file at path: `$HOME/hadoop-2.7.7/etc/hadoop/hdfs-site.xml` on the slave node as follows:

```xml
<configuration>
	<property>
		<name>dfs.replication</name>
		<value>3</value>
	</property>
	<property>
		<name>dfs.permissions</name>
		<value>false</value>
	</property>
	<property>
		<name>dfs.datanode.data.dir</name>
		<value>/home/yitanze2/hadoop-2.7.7/datanode</value>
	</property>
</configuration>
```

10. For all nodes in the cluster, create a new file named `mapred-site.xml` under path: `$HOME/hadoop-2.7.7/etc/hadoop/` and add the following lines to the file.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
<!-- Put site-specific property overrides in this file. -->
<configuration>
	<property>
		<name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>
</configuration>
```

11. For all nodes in the cluster, update the file: `$HOME/hadoop-2.7.7/etc/hadoop/yarn-site.xml` with the following lines.

```xml
<property>
    <name>yarn.resourcemanager.address</name>
    <value>172.22.94.103:8032</value>
</property>
<property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>172.22.94.103:8030</value>
</property>
<property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>172.22.94.103:8031</value>
</property>
<property>
    <name>yarn.resourcemanager.admin.address</name>
    <value>172.22.94.103:8033</value>
</property>
<property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>172.22.94.103:8088</value>
</property>
<property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
</property>
<property>
    <name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
```

12. On the **master** master (only), format the namenode using the command: `hadoop namenode -format`

13. To start hadoop cluster, run `./sbin/start-dfs.sh` and `./sbin/start-yarn.sh` on the **master** node only.

## Quick Start Example

1. Create a file named: `WordCount.java` and copy the following code block to this new file. WordCount is a simple application that counts the number of occurrences of each word in a given input set.

```java
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
```

2. Compile WordCount.java and create a jar:

```
bin/hadoop com.sun.tools.javac.Main WordCount.java

jar cf wc.jar WordCount*.class
```

3. Add files: `file01` and `file02` under **local** directory: `wc-input`. Sample text-files as input:

```
hadoop fs -mkdir -p /wc/input
hadoop fs -put wc-input/* /wc/input

hadoop fs -ls /wc/input
```

4. Run applications using MapReduce of Hadoop:

```
hadoop jar wc.jar WordCount /wc/input /wc/output
hadoop fs -ls /wc/output
hadoop fs -get /wc/output/part-r-00000 wc-output/output.txt
```

Results will be downloaded to the local file: `output.txt`


## FAQ

1. What to do if you run `jps` on slave nodes and cannot find `DataNode` process running?

* Delete `datanode/` directory on slave nodes first
* Re-run format command: `hadoop namenode -format` on the master node
* Start the cluster again then you will see the `DataNode` process.

2. How to debug the cluster setting issue?

* **Always** check log files.

## Reference

* [Hadoop Getting Started](https://hadoop.apache.org/docs/stable/index.html)
* [Hadoop MapReduce Tutorial](https://hadoop.apache.org/docs/r2.9.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)




