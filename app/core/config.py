from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str
    DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_TOPIC: str

    @property
    def KAFKA_DLQ_TOPIC(self) -> str:
        # сюда уезжают сообщения, которые не удалось обработать (dead letter queue)
        return f"{self.KAFKA_TOPIC}.DLQ"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()