from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "FinPulse Nexus API"
    api_prefix: str = "/api"
    quote_cache_ttl_seconds: int = 10  # "live updates" via polling
    max_tickers_per_batch: int = 50

settings = Settings()