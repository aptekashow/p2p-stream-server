from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_port: int = 9999
    acestream_base_url: str = "http://127.0.0.1:6878"
    database_url: str = "sqlite:///./data/p2p_streams.db"
    default_buffer_seconds: int = 10
    hls_segment_seconds: int = 4
    hls_playlist_segments: int = 10
    hls_cache_seconds: int = 300
    hls_cache_poll_seconds: float = 1.5
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
