from feast import Entity, FeatureView, Field, FileSource, FeatureService, Project
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Float32, Int64, String
from datetime import timedelta

# Define a project for the feature repo
project = Project(name="credit_risk_features", description="A project for credit risk features")

# Define entities
customer = Entity(name="customer_id", description="The ID of the customer")
state = Entity(name="state", description="The state of the loan applicant")

# Define FileSources pointing to dbt output (will be replaced by PostgresSource later)
base_customer_source = PostgreSQLSource(
    path="features.base_customer_feature",
    timestamp_field="event_timestamp",
)
rolling_12m_source = PostgreSQLSource(
    path="features.customer_rolling_12m",
    timestamp_field="event_timestamp",
)
geo_aggregates_source = PostgreSQLSource(
    path="features.geo_aggregates_monthly",
    timestamp_field="event_timestamp",
)

# Define Feature Views
base_customer_fv = FeatureView(
    name="base_customer_features",
    entities=[customer],
    ttl=timedelta(days=365 * 2),
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
    source=base_customer_source,
    online=True,
)

rolling_12m_fv = FeatureView(
    name="customer_rolling_12m",
    entities=[customer],
    ttl=timedelta(days=365 * 2),
    schema=[
        Field(name="prior_12m_loan_cnt", dtype=Int64),
        Field(name="prior_12m_loan_amt", dtype=Float32),
        Field(name="prior_12m_avg_int_rate", dtype=Float32),
        Field(name="prior_12m_avg_dti", dtype=Float32),
    ],
    source=rolling_12m_source,
    online=True,
)

geo_aggregates_fv = FeatureView(
    name="geo_aggregates_monthly",
    entities=[state],
    ttl=timedelta(days=365 * 2),
    schema=[
        Field(name="state_rejects_in_month", dtype=Int64),
    ],
    source=geo_aggregates_source,
    online=True,
)

# Define Feature Service
credit_risk_features = FeatureService(
    name="credit_risk_model_features",
    features=[
        base_customer_fv,
        rolling_12m_fv,
        geo_aggregates_fv,
    ],
)