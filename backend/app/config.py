"""
Configuración central del proyecto Global-Relay Sync.
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # PostgreSQL origen
    SOURCE_DB_URL: str = "postgresql://postgres:postgres@localhost:5432/globalrelay"
    
    # PostgreSQL destino (réplica)
    TARGET_DB_URL: str = "postgresql://postgres:postgres@localhost:5432/globalrelay_replica"
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_ORDERS: str = "globalrelay.public.orders"
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379"
    
    # ObserveIQ integration
    OBSERVEIQ_KAFKA_TOPIC: str = "app.logs"
    
    APP_NAME: str = "GlobalRelaySync"
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()