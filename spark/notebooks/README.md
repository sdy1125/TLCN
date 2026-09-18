# Spark notebooks

Notebook files created in Jupyter are persisted in this directory.

The Spark session is preconfigured with the `lakehouse` Iceberg catalog. For example:

```python
spark.sql("SHOW TABLES IN lakehouse.gold").show()
spark.table("lakehouse.gold.commodity_year_summary").show()
```

