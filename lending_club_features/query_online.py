from feast import FeatureStore

# Initialize the FeatureStore
store = FeatureStore(repo_path=".")

# Sample input
entity_row = {"loan_id": "1077430", "state": "CA"}

# Define the features to retrieve
features = [
    "base_customer_features:loan_amount",
    "base_customer_features:fico_band",
    "geo_aggregates_monthly:state_rejects_in_month",
]

# Fetch features
response = store.get_online_features(
    features=features, entity_rows=[entity_row]
).to_df()

# Print result
print(response)
