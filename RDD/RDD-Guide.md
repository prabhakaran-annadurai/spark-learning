## RDD Programming guide - my notes

### Overview

1. RDD - Resilient Distributed Dataset
2. Partitioned across nodes - operated on parallel
3. Creation:
    - Reading a file
    - existing collection in driver program
4. RDDs recover automatically from node failure


### Notes on reading file into RDD

textFile()

1. Each line -> a record 
2. File must be accessible from all nodes
2. Dir / wildcards allowed
3. Second argument - number of partitions - one partition for each block (default 128 MB)

wholeTextFiles()

1. To read multiple small files
2. key - filename, value - content

pickleFile() - For python binary format

sequenceFile() - Hadoop i/p o/p format

### RDD Operations

1. transformations
2. Actions

### Passing functions to spark

1. Lambda expressions - one line
2. Local defs
3. Functions from modules

### OutOfMemory on Driver

collect() fuction tries to bring everything to Driver's memory.

### Types of RDDs

1. Single record RDD
2. Key value pair RDD - Few operations only applicable to this type


### Shuffle

Few operations require shuffling. E.g. reduceByKey

Expensive operation - involves disk i/o, network i/o

### Persists / cache

1. To save intermediate result in memory for faster access.

2. Multiple storage level options available - Memory_only, Disk_only, Memory_and_disk etc

<pre>&gt;&gt;&gt; taxizones = spark.sparkContext.textFile(&quot;file:///data/DataFiles/Raw/TaxiZones.csv&quot;)
&gt;&gt;&gt; taxizonesColRDD = taxizones.map(lambda row: row.split(&quot;,&quot;))
&gt;&gt;&gt; taxizonesColRDD.persist()
PythonRDD[27] at RDD at PythonRDD.scala:56
&gt;&gt;&gt; taxizonesColRDD.filter(lambda row: row[3] == &quot;Airports&quot;).count()
2
</pre>


### Removing Data

1. Auto removal by spark in LRU 
2. Manual removal also possible .unpersist()


### Shared variables

1. Broadcast variables:

- read only, cached on each executors, reused across tasks
- small lookup dataset used across many tasks

<pre>&gt;&gt;&gt; countryCode ={&quot;US&quot; : &quot;United States&quot;,&quot;IN&quot; : &quot;India&quot;}
&gt;&gt;&gt; countryCodeVar = sc.broadcast(countryCode)
&gt;&gt;&gt; countryCodeVar.value
{&apos;US&apos;: &apos;United States&apos;, &apos;IN&apos;: &apos;India&apos;}
&gt;&gt;&gt; countryCodeVar.value.get(&quot;US&quot;)
&apos;United States&apos;
&gt;&gt;&gt; 
</pre>


2. Accumulator:

- For debugging, counting errors
- Not for logic
- Write-Only by executors, Only driver can read

<pre>&gt;&gt;&gt; acc = sc.accumulator(0)
&gt;&gt;&gt; 
&gt;&gt;&gt; rdd = sc.parallelize([1, 2, 3, 4, 5])
&gt;&gt;&gt; rdd.foreach(lambda x: acc.add(x))
&gt;&gt;&gt; 
&gt;&gt;&gt; print(&quot;Total Sum:&quot;, acc.value)  # Output: 15
Total Sum: 15
&gt;&gt;&gt; 
</pre>


