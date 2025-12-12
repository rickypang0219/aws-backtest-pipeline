import matplotlib.pyplot as plt

from io import BytesIO
from typing import TYPE_CHECKING
from matplotlib.figure import Figure


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def save_plot_to_s3(
    s3_client: "S3Client",
    fig: Figure,
    bucket_name: str,
    s3_key: str,
    file_format: str = "pdf",
) -> None:
    buffer = BytesIO()
    fig.savefig(buffer, format=file_format, bbox_inches="tight")
    buffer.seek(0)
    plt.close(fig)

    content_type = "application/pdf" if file_format == "pdf" else "image/png"

    # Upload to S3
    try:
        s3_client.upload_fileobj(
            buffer, bucket_name, s3_key, ExtraArgs={"ContentType": content_type}
        )
        print(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading {s3_key} to S3: {e}")
    finally:
        buffer.close()
