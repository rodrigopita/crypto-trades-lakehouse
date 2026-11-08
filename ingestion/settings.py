from shared.layout import DataLayout


class IngesterSettings(DataLayout):
    uri: str = "wss://ws.kraken.com/v2"

    close_timeout: int = 5
    stall_timeout: int = 5
