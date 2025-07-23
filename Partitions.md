### How spark partition the data

1. when reading files from file system:

- Based on block size of file system
- If HDFS block size is 64MB then 100MB file will have two partitions
- Default size is 128 MB in local and also in HDFS
- Sometimes spark over-partition slightly for better scheduling

    <pre>
    &gt;&gt;&gt; df = spark.read.csv(&quot;/data/Cabs.csv&quot;,header=True)
    &gt;&gt;&gt; df.rdd.getNumPartitions()
    1
    </pre>



2. When creating RDDs using parallelize command:

- Based on total number of cores available in all workers
- sc.defaultParallelism will give the numbers


    <pre>
    &gt;&gt;&gt; sc.defaultParallelism
    3
    &gt;&gt;&gt; rdd=sc.parallelize([1,2])
    &gt;&gt;&gt; rdd.getNumPartitions()
    3
    &gt;&gt;&gt; 
    </pre>


Note: Number of tasks in a stage equals to number of partitions