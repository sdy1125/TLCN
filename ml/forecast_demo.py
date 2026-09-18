"""Very small forecasting example that reads the Gold Iceberg table through Trino."""

import os

import pandas as pd
import trino
from sklearn.linear_model import LinearRegression


connection = trino.dbapi.connect(
    host=os.getenv("TRINO_HOST", "trino"),
    port=int(os.getenv("TRINO_PORT", "8080")),
    user="forecast-demo",
    catalog="iceberg",
    schema="gold",
)

query = """
select year, export_unit_value_usd_per_ton
from commodity_year_summary
where commodity = 'COFFEE'
order by year
"""
frame = pd.read_sql(query, connection)

model = LinearRegression().fit(frame[["year"]], frame["export_unit_value_usd_per_ton"])
next_year = int(frame["year"].max()) + 1
prediction = model.predict(pd.DataFrame({"year": [next_year]}))[0]

print(frame.to_string(index=False))
print(f"Demo forecast for COFFEE in {next_year}: {prediction:,.2f} USD/ton")

