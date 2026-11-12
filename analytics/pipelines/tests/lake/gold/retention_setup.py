from datetime import date

from common.settings import AnalyticsDbSettings
from pipelines.settings import GoldRetentionSettings

DATABASE = {
    "ANALYTICS_DB_URL": "jdbc:postgresql://db:5432/analytics",
    "ANALYTICS_DB_USER": "analytics",
    "ANALYTICS_DB_PASSWORD": "secret",
}
SETTINGS = GoldRetentionSettings(
    analytics_db=AnalyticsDbSettings.from_env(DATABASE), keep_months=36, months_ahead=2
)
AS_OF = date(2026, 11, 10)
QUERY = (
    "SELECT dropped FROM gold_internal.apply_history_retention(36, 2, DATE '2026-11-10') AS dropped"
)
