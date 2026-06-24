# LEAF Streaming Modeler

This repository contains a modeler service compatible with the `leaf-influx` streaming backend of `schema-api`.

The modeler receives time-series data from the LEAF API through the listener, performs any user-defined computation or prediction, and returns prediction columns back to the listener. The listener then converts the modeler response into Influx line protocol and publishes it to MQTT.

---

## 1. Architecture

The flow is:

```text
LEAF API
  -> leaf-listener
  -> modeler
  -> leaf-listener
  -> MQTT write-back
  -> InfluxDB / downstream consumer
```

The modeler does not communicate directly with LEAF, MQTT, or InfluxDB.

The modeler only needs to expose an HTTP endpoint that accepts a JSON request and returns a JSON response.

Default expected endpoint:

```text
POST /model
```

Default port:

```text
8080
```

---

## 2. Modeler responsibility

A modeler is responsible for:

1. Receiving input time series from the listener.
2. Validating the input payload.
3. Running any custom logic, algorithm, or ML model.
4. Returning prediction/output columns.
5. Returning timestamps in UTC epoch seconds.

The modeler may be as simple or as complex as needed. It can:

* use only one input series,
* combine multiple input series,
* align timestamps internally,
* interpolate missing values,
* ignore missing series,
* return no predictions for a window,
* or return many prediction columns.

The important part is that the modeler must follow the input/output protocol described below.

---

## 3. Input protocol: listener to modeler

The listener sends a JSON list of series objects.

Each series object has this shape:

```json
{
  "entity": "D0167289",
  "metric": "Temperature.process-value",
  "time": [1781867217, 1781867227],
  "values": [22.92, 22.85]
}
```

Full example:

```json
[
  {
    "entity": "D0167289",
    "metric": "Temperature.process-value",
    "time": [1781867217, 1781867227],
    "values": [22.92, 22.85]
  },
  {
    "entity": "ssb.bioind4",
    "metric": "mem.used",
    "time": [1781867220, 1781867230],
    "values": [21796589568.0, 21810380800.0]
  }
]
```

### Field meanings

```text
entity
```

The LEAF entity name.

Example:

```text
D0167289
ssb.bioind4
```

```text
metric
```

The LEAF metric name.

Example:

```text
Temperature.process-value
mem.used
```

```text
time
```

A list of timestamps in UTC epoch seconds.

Example:

```json
[1781867217, 1781867227]
```

Important: these timestamps are seconds, not milliseconds and not nanoseconds.

```text
values
```

A list of values corresponding to the `time` list.

Example:

```json
[22.92, 22.85]
```

The modeler should assume:

```text
len(time) == len(values)
```

for every series.

---

## 4. Empty series

The listener may send an empty series if LEAF returned no data for a requested entity/metric pair during the current time window.

Example:

```json
{
  "entity": "ssb.bioind4",
  "metric": "mem.used",
  "time": [],
  "values": []
}
```

This is not necessarily an error.

It means:

```text
No rows were returned for this entity/metric pair during this time window.
```

The modeler can decide how to handle this.

Common strategies:

1. Ignore empty series and predict from the available series.
2. Return no prediction for this time window.
3. Perform interpolation or forward-fill using internal state.
4. Return HTTP 400 if a required input series is missing.

For a production modeler, the recommended behavior depends on the model requirements.

---

## 5. Output protocol: modeler to listener

The modeler must return a JSON object/dictionary.

The response must always contain:

```json
"ts": [...]
```

All other keys are interpreted as output fields/prediction columns.

Example:

```json
{
  "x_pred": [223, 250],
  "y_pred": [104, 108],
  "z_pred": [true, false],
  "ts": [1781867217, 1781867227]
}
```

### Required output rules

The modeler response must follow these rules:

1. The response must be a JSON object.
2. It must contain a `ts` key.
3. `ts` must be a list of UTC epoch seconds.
4. `ts` must contain seconds, not milliseconds or nanoseconds.
5. Every non-`ts` key must be a list.
6. Every output list must have the same length as `ts`.
7. There must be at least one non-`ts` output field.
8. Field names should be valid and meaningful for Influx line protocol.

Valid example:

```json
{
  "x_pred": [223, 250],
  "y_pred": [104, 108],
  "z_pred": [true, false],
  "ts": [1781867217, 1781867227]
}
```

Invalid example because lengths differ:

```json
{
  "x_pred": [223, 250],
  "y_pred": [104],
  "ts": [1781867217, 1781867227]
}
```

Invalid example because `ts` is in nanoseconds:

```json
{
  "x_pred": [223],
  "ts": [1781867217000000000]
}
```

The listener validates the modeler response before publishing to MQTT. If the response is invalid, the listener fails and the streaming task becomes `Error`.

---

## 6. Timestamp contract

The internal timestamp contract is:

```text
LEAF -> listener: ISO timestamp strings
listener -> modeler: UTC epoch seconds
modeler -> listener: UTC epoch seconds
listener -> MQTT/Influx: nanoseconds
```

The modeler should only work with seconds.

Example modeler response:

```json
{
  "x_pred": [223],
  "ts": [1781867217]
}
```

The listener will convert this to Influx line protocol timestamp:

```text
1781867217000000000
```

So the modeler should not multiply timestamps by `1_000_000_000`.

---

## 7. MQTT / Influx output

The modeler does not write to MQTT itself.

The listener converts the modeler response into line protocol.

For example, this modeler output:

```json
{
  "x_pred": [223, 250],
  "y_pred": [104, 108],
  "z_pred": [true, false],
  "ts": [1781867217, 1781867227]
}
```

may become:

```text
model_predictions,workflow=test,producer=leaf-listener,model=dummy-v1 x_pred=223i,y_pred=104i,z_pred=true 1781867217000000000
model_predictions,workflow=test,producer=leaf-listener,model=dummy-v1 x_pred=250i,y_pred=108i,z_pred=false 1781867227000000000
```

Only the modeler output keys become Influx fields.

The listener does not automatically write the input `entity` or `metric` as output fields.

So if the modeler returns:

```json
{
  "x_pred": [223],
  "ts": [1781867217]
}
```

the output field is:

```text
x_pred
```

not:

```text
D0167289__Temperature.process-value
```

---

## 8. Minimal dummy modeler example

Below is a very **simple Flask modeler**.

It receives the list of input series, picks the first non-empty series, and returns three dummy prediction columns:

* `x_pred`
* `y_pred`
* `z_pred`

It returns timestamps in seconds.

```python
from flask import Flask, request, jsonify

app = Flask(__name__)


def first_3_digits_as_int(x):
    """
    Take a number and return its first 3 digits as an int.

    Examples:
      15873966080.0 -> 158
      12.345        -> 123
      -98.76        -> 987
      7             -> 7
    """
    digits = "".join(ch for ch in str(x) if ch.isdigit())

    if not digits:
        return 0

    return int(digits[:3])


@app.route("/model", methods=["POST"])
def model():
    """
    Dummy modeler for the LEAF listener protocol.

    Input:
      [
        {
          "entity": "D0167289",
          "metric": "Temperature.process-value",
          "time": [1781867217, 1781867227],
          "values": [22.92, 22.85]
        }
      ]

    Output:
      {
        "x_pred": [...],
        "y_pred": [...],
        "z_pred": [...],
        "ts": [...]
      }
    """
    payload = request.get_json(force=True)

    if not isinstance(payload, list):
        return jsonify(error="Payload must be a JSON list of series objects"), 400

    print(f"payload: {payload}")

    base_time = []
    base_values = []

    for series in payload:
        if not isinstance(series, dict):
            return jsonify(error="Each series must be a JSON object"), 400

        entity = series.get("entity")
        metric = series.get("metric")
        time_values = series.get("time")
        values = series.get("values")

        if not entity:
            return jsonify(error="Each series must contain 'entity'"), 400

        if not metric:
            return jsonify(error="Each series must contain 'metric'"), 400

        if not isinstance(time_values, list):
            return jsonify(error=f"Series {entity}/{metric}: 'time' must be a list"), 400

        if not isinstance(values, list):
            return jsonify(error=f"Series {entity}/{metric}: 'values' must be a list"), 400

        if len(time_values) != len(values):
            return jsonify(
                error=(
                    f"Series {entity}/{metric}: length mismatch. "
                    f"time has {len(time_values)}, values has {len(values)}"
                )
            ), 400

        # This dummy modeler simply uses the first non-empty input series.
        if not base_time and len(time_values) > 0:
            base_time = time_values
            base_values = values

    x_pred = []
    y_pred = []
    z_pred = []

    for i, value in enumerate(base_values):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            x = first_3_digits_as_int(value)
        elif isinstance(value, str):
            x = first_3_digits_as_int(value)
        else:
            x = 0

        x_pred.append(x)
        y_pred.append(x + 1)
        z_pred.append(i % 2 == 0)

    out_payload = {
        "x_pred": x_pred,
        "y_pred": y_pred,
        "z_pred": z_pred,
        "ts": base_time,
    }

    print(f"out_payload: {out_payload}")

    return jsonify(out_payload)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
```

---

## 9. Requirements

Minimal `requirements.txt`:

```text
flask
```

For more complex modelers, add whatever you need, for example:

```text
flask
numpy
pandas
scikit-learn
```

---

## 10. Dockerfile example

Example `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY modeler.py .

EXPOSE 8080

CMD ["python", "modeler.py"]
```

Build:

```bash
docker build -t your-dockerhub-user/leaf-modeler:latest .
```

Push:

```bash
docker push your-dockerhub-user/leaf-modeler:latest
```

---

## 11. Local testing

Start the modeler:

```bash
python modeler.py
```

Send a test request:

```bash
curl -X POST "http://127.0.0.1:8080/model" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "entity": "D0167289",
      "metric": "Temperature.process-value",
      "time": [1781867217, 1781867227],
      "values": [22.92, 22.85]
    },
    {
      "entity": "ssb.bioind4",
      "metric": "mem.used",
      "time": [1781867220],
      "values": [21796589568.0]
    }
  ]'
```

Expected response:

```json
{
  "ts": [1781867217, 1781867227],
  "x_pred": [229, 228],
  "y_pred": [230, 229],
  "z_pred": [true, false]
}
```

The exact numeric values depend on the dummy logic.

---

## 12. Creating a streaming task with this modeler

To use a modeler in `schema-api`, create a new `leaf-influx` streaming task.

Example request body:

```json
{
  "streaming": "leaf-influx",
  "data": {
    "source": {
      "api_url": "<LEAF_API_URL>",
      "token": "<LEAF_API_TOKEN>",
      "organisation": "WUR",
      "department": "SSB",
      "entity_metrics_set": [
        {
          "entity": "D0167289",
          "metrics": ["Temperature.process-value"]
        },
        {
          "entity": "ssb.bioind4",
          "metrics": ["mem.used"]
        }
      ],
      "everyTs": 20,
      "queryDelayS": 30,
      "limit": 100
    },
    "modeler": {
      "image": "<your-dockerhub-modeler-image>:<tag>",
      "port": 8080,
      "endpoint": "model",
      "args": ["python", "modeler.py"]
    },
    "mqtt": {
      "host": "<MQTT_HOST>",
      "port": 443,
      "username": "<MQTT_USERNAME>",
      "password": "<MQTT_PASSWORD>",
      "topic": "<MQTT_TOPIC>",
      "basepath": "mqtt",
      "measurement": "model_predictions",
      "output_tags": [
        "workflow=test",
        "producer=leaf-listener",
        "model=dummy-v1"
      ]
    }
  }
}
```

Field notes:

* `source.api_url`: LEAF API endpoint.
* `source.token`: LEAF API token.
* `source.organisation`: LEAF organisation.
* `source.department`: LEAF department.
* `source.entity_metrics_set`: explicit entity/metric pairs to request from LEAF.
* `source.everyTs`: polling window size in seconds.
* `source.queryDelayS`: delay the queried window by this many seconds, useful when LEAF data appears with a small delay.
* `source.limit`: maximum number of LEAF rows returned per polling window.
* `modeler.image`: Docker image of the modeler.
* `modeler.port`: port exposed by the modeler container.
* `modeler.endpoint`: HTTP endpoint exposed by the modeler, usually `model`.
* `modeler.args`: command used to start the modeler inside the container.
* `mqtt.host`: MQTT broker host.
* `mqtt.port`: MQTT broker port.
* `mqtt.username`: MQTT username.
* `mqtt.password`: MQTT password.
* `mqtt.topic`: MQTT topic where line protocol rows will be published.
* `mqtt.basepath`: MQTT WebSocket path, usually `mqtt`.
* `mqtt.measurement`: Influx line protocol measurement name.
* `mqtt.output_tags`: tags added to every line protocol row.

The listener will create one LEAF API request per polling window, send the resulting time-series payload to the modeler, receive modeler predictions, convert them to Influx line protocol, and publish them to MQTT.

---

## 13. Error handling

If the modeler receives invalid input, it should return HTTP 400 with a helpful error message.

Example:

```json
{
  "error": "Payload must be a JSON list of series objects"
}
```

If the modeler returns an invalid response, the listener will fail before MQTT publish.

Invalid modeler response examples:

```json
{
  "x_pred": [1, 2],
  "y_pred": [3],
  "ts": [1781867217, 1781867227]
}
```

```json
{
  "x_pred": [1],
  "ts": [1781867217000000000]
}
```

The first fails because output lengths differ.

The second fails because `ts` looks like nanoseconds instead of seconds.

---

## 14. Production notes

For a production modeler:

1. Do not rely blindly on input order unless your model explicitly expects it.
2. Validate required entity/metric pairs.
3. Decide how to handle missing or empty series.
4. Decide whether to align by exact timestamp, nearest timestamp, interpolation, or forward-fill.
5. Return timestamps in seconds.
6. Return only output columns that should be written as Influx fields.
7. Avoid returning huge payloads in one response.
8. Log enough information for debugging, but never log secrets.
9. Use a production WSGI server if needed, such as Gunicorn.

Example Gunicorn command:

```bash
gunicorn -b 0.0.0.0:8080 modeler:app
```

Corresponding schema-api modeler args:

```json
"args": ["gunicorn", "-b", "0.0.0.0:8080", "modeler:app"]
```

---

## 15. Summary

The modeler input is:

```json
[
  {
    "entity": "...",
    "metric": "...",
    "time": [epoch_seconds],
    "values": [...]
  }
]
```

The modeler output is:

```json
{
  "some_prediction_field": [...],
  "another_prediction_field": [...],
  "ts": [epoch_seconds]
}
```

The listener handles MQTT write-back.

The modeler only needs to implement the `/model` HTTP endpoint and follow the JSON protocol.
