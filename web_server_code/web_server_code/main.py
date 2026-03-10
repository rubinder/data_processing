"""FastAPI web server that serves impression data as gzip CSV."""

from fastapi import FastAPI, Query, Response

from web_server_code.generator import generate_gzip_csv

app = FastAPI(title="Impression Data Server")


@app.get("/impression")
def get_impression(
    page_type: int = Query(..., ge=1, le=3, description="Page type (1, 2, or 3)"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    hour: int = Query(..., ge=0, le=23, description="Hour (0-23)"),
) -> Response:
    """Generate and return impression data as a gzip-compressed CSV file."""
    content = generate_gzip_csv(page_type, date, hour)
    return Response(
        content=content,
        media_type="application/gzip",
        headers={
            "Content-Disposition": (
                f"attachment; filename=impressions_"
                f"pt{page_type}_{date}_h{hour}.csv.gz"
            ),
        },
    )
