from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "FinPulse Nexus API"
    api_prefix: str = "/api"
    quote_cache_ttl_seconds: int = 10
    max_tickers_per_batch: int = 50

    database_url: str
    gemini_api_key: str = ""
    finnhub_api_key: str = ""
    twelve_data_api_key: str = ""

    upstox_analytics_token: str = ""
    upstox_base_url: str = "https://api.upstox.com/v2"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()