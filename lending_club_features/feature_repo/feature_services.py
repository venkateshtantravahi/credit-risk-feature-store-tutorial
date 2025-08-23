from feast import FeatureService

from .feature_views import base_customer_features, geo_aggregates_monthly

lending_club_services = FeatureService(
    name="lending_club_services",
    features=[base_customer_features, geo_aggregates_monthly],
)
