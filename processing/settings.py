from shared.layout import DataLayout


class ProcessingSettings(DataLayout):
    shuffle_partitions: int = 2
    await_termination_timeout: int = 2

    silver_watermark_threshold: str = "5 minutes"
    gold_watermark_threshold: str = "1 minute"

    kraken_base_url: str = "https://api.kraken.com/0"
    request_timeout: int = 60
