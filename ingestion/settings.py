from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    uri: str = "wss://ws.kraken.com/v2"
    landing_folder: str = "data/landing"
    tmp_folder: str = "data/tmp"
    record_folder: str = "data/record"
