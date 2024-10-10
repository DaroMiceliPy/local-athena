from Athena import Athena
from datetime import datetime

class ny_data(Athena):
    today = datetime.utcnow()
    _settings = {
        "table": "ny_data",
        "partitions": [
            "downloaded_at"
        ]
    }
    filters = {
        "downloaded_at": f"{today.strftime('%Y-%m-%d')}"
    }

    url = {
        "bucket": "DataLake",
        "path": f"ny_data/downloaded_at={today.strftime('%Y-%m-%d')}",
        "filename": "ny_data.parquet"
    }


    _schema = {
        "address": {
            "dtype": "string",
        },
        "residential_units": {
            "dtype": "int"
        },
        "sale_price": {
            "dtype": "int"
        },
        "sale_date": {
            "dtype": "string"
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    