import requests

from pathlib import Path
from pydantic import BaseModel
from data_processing.int_trades.shared import logger


class DownloadRawDataEvent(BaseModel):
    coin_name: str
    zip_url: str
    zip_path: str


def streaming_data_downloader(
    zip_url: str,
    zip_path: str,
) -> str:
    try:
        response = requests.get(zip_url, stream=True, timeout=300)
        response.raise_for_status()

        directory_path = Path(f"/tmp/{zip_path}").parent  # noqa: S108
        directory_path.mkdir(parents=True, exist_ok=True)
        with Path(f"/tmp/{zip_path}").open("wb") as f:  # noqa: S108
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        logger.info("File successfully downloaded to %s", zip_path)
        return zip_path
    except requests.exceptions.RequestException as e:
        logger.error("Error downloading file: %s", e)
        raise
    except OSError as e:
        logger.error("Error writing file to %s:%s", zip_path, e)
        raise


if __name__ == "__main__":
    from data_processing.int_trades.handlers import generate_tasks_handler

    event = {
        "coin_name": "BTCUSDT",
        "start_year": 2025,
        "start_month": 1,
        "end_year": 2025,
        "end_month": 6,
        "base_s3_prefix": "raw_data",
    }
    tasks = generate_tasks_handler(event, event)
    test_case = tasks["tasks"][0]
    streaming_data_downloader(test_case["zip_url"], test_case["zip_path"])
