from Athena import Athena
from datetime import datetime


class financial_data(Athena):
    today = datetime.utcnow()
    _settings = {
        "table": "financial_data",
        "partitions": [
            "year",
	    "month",
	    "day"
        ]
    }
    filters = {
        "year": f"{today.strftime('%Y')}",
        "month": f"{today.strftime('%m')}",
        "day": f"{today.strftime('%d')}",
    }

    url = {
        "bucket": "DataLake",
        "path": f"financial_data/year={today.strftime('%Y')}/month={today.strftime('%m')}/day={today.strftime('%d')}",
        "filename": "financial_data.parquet"
    }


    _schema = {
        "Open": {
            "dtype": "double",
        },
        "High": {
            "dtype": "double"
        },
        "Low": {
            "dtype": "double"
        },
        "Close": {
            "dtype": "double"
        },
        "Adj_Close": {
            "dtype": "double"
        },
        "Volume": {
            "dtype": "double"
        },
        "Symbol": {
            "dtype": "string"
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)