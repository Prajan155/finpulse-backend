from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "FinPulse Nexus API"
    api_prefix: str = "/api"
    quote_cache_ttl_seconds: int = 10
    max_tickers_per_batch: int = 50

    database_url: str
    gemini_api_key: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()