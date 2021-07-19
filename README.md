# Mini Project - Spark Optimization

This is an analysis of a Python script that needs to be optimized for Spark. 

Input FIles:

* questions.parquet
* answers.parquet

Query purpose: Get the number of answers by month for each question. 

Analysis objective: Rewrite script to achieve optimal performance.

Frameworks: Pyspark

Spark Mode: Single Node (Local)

## Physical Plan

Original query shows 2 shuffles for the following operations:

* GroupBy operation
* Join operation

Since the Join strategy being used is BroadcastHashJoin, the join strategy with the best performance, we can't improve this operation in particular. The target is to eliminate the shuffle associated with the GroupBy operation.

## Approaches

### Checking Operators

Assuming that we want to have the following fields in the final table, we are using the right operators already.

* question_id: unique id of the question
* creation_date: date of creation of the question
* title: question
* month: month of answer
* cnt: count of answers per month

Operators are:

* groupBy question_id and month / aggregation by count from answers table
* inner join of questions table with answer aggregated table

For the Inner Join, the Largest table is already on the left side of the Join, so this optimized already (validated with the fact that we have a BroadcastHashJoin as strategy being used)


### Checking Resource Allocation

Resource allocation is optimized already:

* spark.driver.cores (default = 1g): Only applicable in cluster mode
* spark.driver.memory (default = 1g): No out of memory exceptions 
* executor.memory (default = 1g): No out of memory exceptions 
* executor.cores (default= all avail. Cores in local mode): No more cores to assign


### Number of Partitions

Input files are partitioned evenly (no skew in ingestion of data). 

Partitions number is 4. CPU has 4 cores, so 4 partitions is the minimum to have as a rule of thumb. Repartitioning would cause more shuffling, which is what we are trying to eliminate in this case.

### Other considerations

* Reduction of Data Structure Size doesn't apply.
* Parquet format is the best format for OLAP workloads in Spark.

### Reduce the number of shuffles and the amount of data shuffled


Strategies tried:

* answersDF was cached without success
* SQL Adaptive feature was enabled without success. There was reduction in number of tasks though
* Broadcasting was not attempted, as this more suitable for small DataFrames or for variables

* Bucketing was implemented for the answers table by question_Id, one of the fields being used for the GroupBy operation, and also the field used for the Join. This approach eliminated the large Exchange (shuffle) previously seen.

Backup file with steps can be found [here][1]

[1]: https://docs.google.com/presentation/d/1vQxXIslhtIDmsZSyr3zlrlzYq_xs4YBLnP5CBRfC68U/edit?usp=sharing


## Authors

Rafael Roano