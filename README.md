This is just an expirement. S4 is a placeholder name.

The goal is to experiment with enhancementd to S3 built on top of S3.

I'm going to start with caching Parquet metadata in Redis.
My local results show a ~20% speedup by enabling the cache (for a maybe pathological use case? it is not real world)

Next I'd like to see if I can cache file lists for paths of plain/hive parquet tables.
After that, can we cache and accelerate DeltaLake/Iceberg metadata?
Can we buffer writes of small parquet files and compact on write?
