from shared.layout import DataLayout


class ProcessingSettings(DataLayout):
    watermark_threshold: str = "5 minutes"
