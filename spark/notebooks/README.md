# Spark notebooks

Notebook files created in Jupyter are persisted in this directory.

The Spark session is preconfigured with the `lakehouse` Iceberg catalog. Silver V2 data can be inspected with:

```python
spark.sql("SHOW TABLES IN lakehouse.silver_core").show()
spark.table("lakehouse.silver_core.production_observation").show(5)
```

