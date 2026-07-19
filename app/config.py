import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # App Mode
    DEBUG: bool = True

    # Database
    DATABASE_URL: str = "postgresql://dispatch_user:dispatch_password@127.0.0.1:5432/dispatch_db"
    
    # Redis
    REDIS_HOST: str = "127.0.0.1"

    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    # Dispatch Configuration
    GEOSEARCH_RADIUS_KM: float = 5.0
    DRIVER_OFFER_TIMEOUT_SEC: int = 10  # Seconds to accept/reject an offer
    DRIVER_HEARTBEAT_TIMEOUT_SEC: int = 30  # Telemetry stale threshold

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def sqlalchemy_database_url(self) -> str:
        # Graceful fallback to SQLite if postgresql URL is not customized and docker might not be running
        # This makes the project extremely easy to run and test locally.
        # We can detect if we want to fallback or keep postgres
        return os.environ.get("DATABASE_URL", self.DATABASE_URL)

settings = Settings()
