from pydantic_settings import BaseSettings


class DataLayout(BaseSettings):
    data_folder: str = "data"
    asset_pair: str = "BTC/USD"

    def landing_folder(self) -> str:
        return f"{self.data_folder}/landing"

    def archive_folder(self) -> str:
        return f"{self.data_folder}/archive"

    def tmp_folder(self) -> str:
        return f"{self.data_folder}/tmp"

    def table_path(self, layer: str, table: str) -> str:
        return f"{self.data_folder}/{layer}/{table}"

    def query_checkpoint_path(self, layer: str, table: str) -> str:
        return f"{self.data_folder}/checkpoints/{layer}/{table}"

    def record_path(self) -> str:
        return f"{self.data_folder}/record/written.jsonl"

    def progress_metrics_path(self) -> str:
        return f"{self.data_folder}/metrics/progress.jsonl"

    def connections_metrics_path(self) -> str:
        return f"{self.data_folder}/metrics/connections.jsonl"
