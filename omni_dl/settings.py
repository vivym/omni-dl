from pydantic import BaseModel, BaseSettings


class StorageOptions(BaseModel):
    endpoint_url: str | None = None
    key: str | None = None
    secret: str | None = None


class Settings(BaseSettings):
    log_level: str = "INFO"

    # Settings for Server
    secret_key: str = "secret"
    secret_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "omni_dl"

    celery_app_name: str = "omni-dl-server-tasks"
    celery_broker_uri: str = "redis://localhost:6379/0"
    celery_backend_uri: str = "redis://localhost:6379/0"

    # Settings for Client
    num_workers: int = 4
    refresh_interval: int = 5

    api_url: str = "https://api.dl.sota.wiki"
    username: str | None = None
    password: str | None = None

    storage_uri: str = "s3://omni_dl"
    storage_options: dict[str, str] | None = None

    class Config:
        env_nested_delimiter = "__"


settings = Settings(_env_file=".env")
