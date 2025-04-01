import logging
import gzip
import json
import os
import io
import psycopg2
import azure.functions as func
from datetime import datetime, timezone

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("OTEL ingest function hit.")

    try:
        # Handle gzip content
        if req.headers.get('Content-Encoding') == 'gzip':
            buf = io.BytesIO(req.get_body())
            with gzip.GzipFile(fileobj=buf, mode='rb') as f:
                body = f.read().decode('utf-8')
        else:
            body = req.get_body().decode('utf-8')

        data = json.loads(body)
        logging.info("Payload parsed.")

        # Parse OTEL metrics
        records = []
        for resourceBlock in data.get("resourceMetrics", []):
            resourceAttrs = _parse_attributes(resourceBlock.get("resource", {}).get("attributes", []))
            host_id = resourceAttrs.get("hostId", "unknown")

            for scopeBlock in resourceBlock.get("scopeMetrics", []):
                scopeAttrs = _parse_attributes(scopeBlock.get("scope", {}).get("attributes", []))
                data_source_id = scopeAttrs.get("datasourceId", "unknown")

                for metric in scopeBlock.get("metrics", []):
                    metric_name = metric.get("name", "unknown")
                    points = (
                        metric.get("gauge", {}).get("dataPoints", []) +
                        metric.get("sum", {}).get("dataPoints", [])
                    )

                    for point in points:
                        value = point.get("asDouble", 0)
                        ts = _convert_unix_nano_to_datetime(point.get("timeUnixNano"))
                        attr_json = json.dumps(point.get("attributes", []))
                        records.append((host_id, data_source_id, metric_name, value, ts, attr_json))

        if not records:
            logging.warning("No metrics to insert.")
            return func.HttpResponse("No metrics found.", status_code=200)

        # Connect to PostgreSQL
        conn = psycopg2.connect(os.environ["PostgresConnectionString"])
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO otel_metrics (host_id, data_source_id, metric_name, metric_value, timestamp_utc, attributes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, records)
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"{len(records)} metrics inserted.")
        return func.HttpResponse(f"{len(records)} metrics inserted.", status_code=200)

    except Exception as e:
        logging.error(f"Function error: {str(e)}", exc_info=True)
        return func.HttpResponse("Error processing OTEL data", status_code=500)

def _parse_attributes(attr_list):
    return {
        item["key"]: item["value"].get("stringValue", "")
        for item in attr_list
    }

def _convert_unix_nano_to_datetime(unix_nano):
    try:
        return datetime.fromtimestamp(int(unix_nano) / 1e9, tz=timezone.utc)
    except:
        return None
