from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    uri: str = "wss://ws.kraken.com/v2"

    landing_folder: str = "data/landing"
    tmp_folder: str = "data/tmp"
    record_folder: str = "data/record"

    close_timeout: int = 5
    stall_timeout: int = 5
