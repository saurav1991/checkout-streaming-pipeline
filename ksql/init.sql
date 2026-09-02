-- Create a stream over the raw pageviews topic
CREATE STREAM IF NOT EXISTS pageviews_stream (
  user_id INT,
  postcode VARCHAR,
  webpage VARCHAR,
  event_time BIGINT
) WITH (
  KAFKA_TOPIC='pageviews',
  VALUE_FORMAT='JSON',
  TIMESTAMP='event_time'
);

-- Create aggregation table with 1-minute tumbling window, grouped by webpage and postcode
CREATE TABLE IF NOT EXISTS pageview_aggregates
WITH (KAFKA_TOPIC='pageview_aggregates', KEY_FORMAT='JSON', VALUE_FORMAT='JSON') AS
SELECT
  webpage,
  postcode,
  AS_VALUE(webpage) AS webpage_value,
  AS_VALUE(postcode) AS postcode_value,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  COUNT(*) AS pageview_count
FROM pageviews_stream
WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 30 SECONDS)
GROUP BY webpage, postcode
EMIT FINAL;
