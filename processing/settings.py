from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    data_folder: str = "data"

    def landing_path(self) -> str:
        return f"{self.data_folder}/landing"

    def table_path(self, layer: str, table: str) -> str:
        return f"{self.data_folder}/{layer}/{table}"

    def checkpoint_path(self, layer: str, table: str) -> str:
        return f"{self.data_folder}/checkpoints/{layer}/{table}"
