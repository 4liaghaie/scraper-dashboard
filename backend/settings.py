from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr, EmailStr, field_validator

class Settings(BaseSettings):
    # Core
    database_url: str = Field(..., validation_alias="DATABASE_URL")

    # Accept JSON array, CSV string, or single string
    cors_origins: list[str] | str = Field(default="*", alias="CORS_ORIGINS")

    # JWT
    jwt_secret: SecretStr = Field(..., validation_alias="JWT_SECRET")
    jwt_algorithm: str = Field("HS256", validation_alias="JWT_ALGORITHM")
    access_token_minutes: int = Field(60, validation_alias="ACCESS_TOKEN_MINUTES")

    # Optional paths
    rebaid_categories_path: str | None = Field(None, validation_alias="REBAID_CATEGORIES_PATH")
    myvipon_categories_path: str | None = Field(None, validation_alias="MYVIPON_CATEGORIES_PATH")

    # Bootstrap admin (optional)
    superuser_email: EmailStr | None = Field(None, validation_alias="SUPERUSER_EMAIL")
    superuser_password: SecretStr | None = Field(None, validation_alias="SUPERUSER_PASSWORD")

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", case_sensitive=False)

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _normalize_cors(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            # already a list
            return [x.strip() for x in v if isinstance(x, str) and x.strip()]
        if isinstance(v, str):
            s = v.strip()
            if s == "" or s == "*":
                return ["*"]
            # try JSON array first
            if s.startswith("["):
                import json
                try:
                    arr = json.loads(s)
                    return [x.strip() for x in arr if isinstance(x, str) and x.strip()]
                except Exception:
                    # fall back to CSV
                    pass
            # CSV or single origin
            return [x.strip() for x in s.split(",") if x.strip()]
        # anything else -> empty
        return []

settings = Settings()
