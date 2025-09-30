from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr, EmailStr

class Settings(BaseSettings):
    database_url: str = Field(..., validation_alias="DATABASE_URL")
    cors_origins: list[str] = Field(default_factory=list, alias="CORS_ORIGINS")
    # JWT
    jwt_secret: SecretStr = Field(..., validation_alias="JWT_SECRET")
    jwt_algorithm: str = Field("HS256", validation_alias="JWT_ALGORITHM")
    access_token_minutes: int = Field(60, validation_alias="ACCESS_TOKEN_MINUTES")
    rebaid_categories_path: str | None = Field(None, validation_alias="REBAID_CATEGORIES_PATH")
    myvipon_categories_path: str | None = Field(None, validation_alias="MYVIPON_CATEGORIES_PATH")
    # Superuser bootstrap (optional; used once on empty DB)
    superuser_email: EmailStr | None = Field(None, validation_alias="SUPERUSER_EMAIL")
    superuser_password: SecretStr | None = Field(None, validation_alias="SUPERUSER_PASSWORD")

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", case_sensitive=False)
    @property
    def cors_origins_normalized(self) -> list[str]:
        if self.cors_origins and isinstance(self.cors_origins, str):
            # If someone sets a raw string by mistake, split it
            return [o.strip() for o in self.cors_origins.split(",") if o.strip()]
        return self.cors_origins

    # expose a single name used above
    @property
    def cors_origins(self) -> list[str]:
        return self.cors_origins_normalized
settings = Settings()
