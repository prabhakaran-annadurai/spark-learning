## Streaming example - console

<pre>&gt;&gt;&gt; lines = spark.readStream.format(&quot;socket&quot;).option(&quot;host&quot;,&quot;localhost&quot;).option(&quot;port&quot;,9001).load()
25/07/18 15:47:46 WARN TextSocketSourceProvider: The socket source should not be used for production applications! It does not support recovery.
&gt;&gt;&gt; words = lines.select(explode(split(lines.value,&quot; &quot;)).alias(&quot;word&quot;))
&gt;&gt;&gt; wordCounts = words.groupBy(&quot;word&quot;).count()
&gt;&gt;&gt; query = wordCounts.writeStream.outputMode(&quot;complete&quot;).format(&quot;console&quot;).start()
25/07/18 15:47:56 WARN ResolveWriteToStream: Temporary checkpoint location created which is deleted normally when the query didn&apos;t fail: /tmp/temporary-5a7b76d4-6ff0-4361-a39f-c5e57df8a965. If it&apos;s required to delete it under any circumstances, please set spark.sql.streaming.forceDeleteTempCheckpointLocation to true. Important to know deleting temp checkpoint folder is best effort.
25/07/18 15:47:56 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+----+-----+
|word|count|
+----+-----+
+----+-----+

-------------------------------------------                                     
Batch: 1
-------------------------------------------
+-----+-----+
| word|count|
+-----+-----+
|  you|    1|
|  how|    1|
|hello|    1|
|  are|    1|
+-----+-----+

-------------------------------------------                                     
Batch: 2
-------------------------------------------
+-----+-----+
| word|count|
+-----+-----+
|  you|    2|
|  how|    1|
|hello|    1|
| here|    1|
|  are|    2|
+-----+-----+

</pre>



<pre><font color="#54FF54"><b>ubuntu@spark-master</b></font>:<font color="#5454FF"><b>~</b></font>$ nc -lk 9001
hello how are you
you are here

</pre>