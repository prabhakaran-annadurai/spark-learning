## Spark APIs

1. RDD API: low level, spark doesn't apply optimizations on our code

2. Dataframes: 
    - Spark 1.3, based on RDDs
    - Tabular format
    - No compile time safety
    - Spark applies optimizations

3. Datasets:
    - Spark 1.6 based on RDDs
    - Provides compile time safety 
    - supported only in Java, Scala

4. Structured APIs:
    - Merged Dataframes & Datasets
    - From Spark 2.0
    - Strongly styped APIs - Java, Scala
    - Untyped APIs - Python & R

Spark core:

        - Provides APIs for RDDs
        - Accumulators, Broadcast
        - Task and Job submission

Spark-SQL:

        - Spark-module note spark-sql command line tool 
        - Provides structured data processing
        - Applies optimizations on top of RDDs


## Execution flow


--> Python code in pyspark
    
    --> Parsed Logical plan

        --> Analyzed logical plan

            --> Optimized logical plan

                --> Physical plan


1. Dataframe -> Parsed Logical plan

    - by Parser (Catalyst)
    - Abstract Syntax Tree generated from code
    - No validations on column names or types

2. Parsed logical plan -> Analyzed logical plan

    - by Analyzer (Catalyst)
    - Resolves column names, data types (casting is done if required)
    - No optimizations yet

3. Analyzed logical plan -> Optimized logical plan

    - by Catalyst optimizer
    - Optimizations applied:
    
    For example:

        - Removes unused columns from pruning
        - Filter reordering to reduce number of rows early
        - Null filter optimizations

4. Optimized logical plan -> Physical plan

    - by Planner (Catalyst + Tungsten)
    - An executable DAG of Physical operations
    - Sent to spark-core and run on RDDs


<pre>&gt;&gt;&gt; 
&gt;&gt;&gt; df = spark.read.csv(&quot;/data/Cabs.csv&quot;,header=True)
&gt;&gt;&gt; df3= df.select(&quot;CabNumber&quot;).filter(&quot;VehicleYear &gt; 2016&quot;)
&gt;&gt;&gt; df3.explain(extended=True)
== Parsed Logical Plan ==
&apos;Filter (&apos;VehicleYear &gt; 2016)
+- Project [CabNumber#85]
   +- Relation [CabNumber#85,VehicleLicenseNumber#86,Name#87,LicenseType#88,Active#89,PermitLicenseNumber#90,VehicleVinNumber#91,WheelchairAccessible#92,VehicleYear#93,VehicleType#94,TelephoneNumber#95,Website#96,Address#97,LastDateUpdated#98] csv

== Analyzed Logical Plan ==
CabNumber: string
Project [CabNumber#85]
+- Filter (cast(VehicleYear#93 as bigint) &gt; cast(2016 as bigint))
   +- Project [CabNumber#85, VehicleYear#93]
      +- Relation [CabNumber#85,VehicleLicenseNumber#86,Name#87,LicenseType#88,Active#89,PermitLicenseNumber#90,VehicleVinNumber#91,WheelchairAccessible#92,VehicleYear#93,VehicleType#94,TelephoneNumber#95,Website#96,Address#97,LastDateUpdated#98] csv

== Optimized Logical Plan ==
Project [CabNumber#85]
+- Filter (isnotnull(VehicleYear#93) AND (cast(VehicleYear#93 as bigint) &gt; 2016))
   +- Relation [CabNumber#85,VehicleLicenseNumber#86,Name#87,LicenseType#88,Active#89,PermitLicenseNumber#90,VehicleVinNumber#91,WheelchairAccessible#92,VehicleYear#93,VehicleType#94,TelephoneNumber#95,Website#96,Address#97,LastDateUpdated#98] csv

== Physical Plan ==
*(1) Project [CabNumber#85]
+- *(1) Filter (isnotnull(VehicleYear#93) AND (cast(VehicleYear#93 as bigint) &gt; 2016))
   +- FileScan csv [CabNumber#85,VehicleYear#93] Batched: false, DataFilters: [isnotnull(VehicleYear#93), (cast(VehicleYear#93 as bigint) &gt; 2016)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/data/Cabs.csv], PartitionFilters: [], PushedFilters: [IsNotNull(VehicleYear)], ReadSchema: struct&lt;CabNumber:string,VehicleYear:string&gt;

&gt;&gt;&gt; 
</pre>