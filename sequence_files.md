#### Sequence Files

What are they ?

Binary file, used in Hadoop ecosystem, contains key, value pairs.

They provide following options:

1. Key, value pair storage - each file has metadata information
2. Splittable - Multiple partition files created.
3. Sync markers at every 2 MB, used for resuming reads and enables splitting


Examples:

Consider a key, value pair RDD like below,

<pre>&gt;&gt;&gt; rdd = sc.parallelize([(1,1),(2,4),(3,9),(4,16),(5,25)])
&gt;&gt;&gt; rdd.saveAsSequenceFile(&quot;file:///tmp/sequencefiles&quot;)
</pre>

It creates a folder under tmp with like below

<pre><font color="#54FF54"><b>buntu@spark-master</b></font>:<font color="#5454FF"><b>/tmp/sequencefiles</b></font>$ ls -lrt
total 12
-rw-r--r-- 1 ubuntu ubuntu 108 Jul 15 21:08 part-00000
-rw-r--r-- 1 ubuntu ubuntu 124 Jul 15 21:08 part-00002
-rw-r--r-- 1 ubuntu ubuntu 124 Jul 15 21:08 part-00001
-rw-r--r-- 1 ubuntu ubuntu   0 Jul 15 21:08 _SUCCESS
<font color="#54FF54"><b>ubuntu@spark-master</b></font>:<font color="#5454FF"><b>/tmp/sequencefiles</b></font>$ cat part-00000
SEQ org.apache.hadoop.io.IntWritable org.apache.hadoop.io.IntWritableH��w���crS�</pre>


This can be read from spark session and creates a RDD as follows,

<pre>&gt;&gt;&gt; rdd2 = sc.sequenceFile(&quot;file:///tmp/sequencefiles&quot;)
&gt;&gt;&gt; rdd2.collect()
[(2, 4), (3, 9), (1, 1), (4, 16), (5, 25)]
&gt;&gt;&gt; 
</pre>