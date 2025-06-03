"""Configuration management for the application."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    # Flask settings
    SECRET_KEY: str
    DEBUG: bool
    ENV: str

    # Server settings
    HOST: str
    PORT: int

    # CORS settings
    CORS_ORIGINS: str

    # Gunicorn settings
    WORKERS: int
    TIMEOUT: int
    WORKER_CLASS: str
    LOG_LEVEL: str

    # Application paths
    LOG_DIR: str
    STATIC_DIR: str
    TEMPLATE_DIR: str

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        env = os.getenv("FLASK_ENV", "development")

        return cls(
            SECRET_KEY=os.getenv("FLASK_SECRET_KEY", os.urandom(24).hex()),
            DEBUG=env == "development",
            ENV=env,
            HOST=os.getenv("HOST", "0.0.0.0"),
            PORT=int(os.getenv("PORT", "8000")),
            CORS_ORIGINS=os.getenv("ALLOWED_ORIGINS", "*"),
            WORKERS=int(os.getenv("GUNICORN_WORKERS", "3")),
            TIMEOUT=int(os.getenv("GUNICORN_TIMEOUT", "120")),
            WORKER_CLASS=os.getenv("GUNICORN_WORKER_CLASS", "gevent"),
            LOG_LEVEL=os.getenv("LOG_LEVEL", "info"),
            LOG_DIR=os.getenv("LOG_DIR", "logs"),
            STATIC_DIR=os.getenv("STATIC_DIR", "static"),
            TEMPLATE_DIR=os.getenv("TEMPLATE_DIR", "templates"),
        )


# Default configurations
class DevConfig:
    """Development configuration defaults."""

    ENV = "development"
    DEBUG = True
    HOST = "localhost"
    WORKERS = 1
    LOG_LEVEL = "debug"


class ProdConfig:
    """Production configuration defaults."""

    ENV = "production"
    DEBUG = False
    CORS_ORIGINS = ""  # Should be set to specific domain
    WORKERS = 3
    LOG_LEVEL = "info"


# Load config based on environment
config = Config.from_env()
