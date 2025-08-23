from feast import FeatureView, Field
from feast.types import Float32, Int64, String
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from datetime import timedelta
from .entities import customer, state


base_customer_features = FeatureView(
    name="base_customer_features",
    entities=[customer],
    ttl=timedelta(days=365),
    schema=[
        Field(name="loan_amount", dtype=Float32),
        Field(name="funded_amount", dtype=Float32),
        Field(name="annual_income", dtype=Float32),
        Field(name="dti", dtype=Float32),
        Field(name="int_rate_pct", dtype=Float32),
        Field(name="revol_util_pct", dtype=Float32),
        Field(name="fico_avg", dtype=Float32),
        Field(name="fico_band", dtype=String),
    ],
    source=PostgreSQLSource(
        name="base_customer_features",
        query="SELECT * FROM credit_risk.staging_features.base_customer_features",
        timestamp_field="event_timestamp",
    ),
)


geo_aggregates_monthly = FeatureView(
    name="geo_aggregates_monthly",
    entities=[state],
    ttl=timedelta(days=365),
    schema=[
        Field(name="state_rejects_in_month", dtype=Int64),
    ],
    source=PostgreSQLSource(
        name="geo_aggregates_monthly",
        query="SELECT * FROM credit_risk.staging_features.geo_aggregates_monthly",
        timestamp_field="event_timestamp",
    ),
)
