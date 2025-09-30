from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr, EmailStr
from pydantic import Field, field_validator

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
    @field_validator("cors_origins", mode="before")
    @classmethod
    def _normalize_cors(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            s = v.strip()
            # If JSON array string
            if s.startswith("["):
                import json
                try:
                    arr = json.loads(s)
                    return [x.strip() for x in arr if isinstance(x, str) and x.strip()]
                except Exception:
                    # fall through to CSV parsing
                    pass
            # CSV or single origin
            if s == "" or s == "*":
                return ["*"]
            return [x.strip() for x in s.split(",") if x.strip()]
        return []

settings = Settings()