from shared.layout import DataLayout


class ProcessingSettings(DataLayout):
    silver_watermark_threshold: str = "5 minutes"
    gold_watermark_threshold: str = "1 minute"
