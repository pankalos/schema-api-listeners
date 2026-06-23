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
    Dummy modeler for the new listener protocol.

    Input:
      [
        {
          "entity": "D0167289",
          "metric": "Temperature.process-value",
          "time": [1781690000, 1781690002],
          "values": [23.325, 23.34]
        }
      ]

    Output:
      {
        "x_pred": [...],
        "y_pred": [...],
        "z_pred": [...],
        "ts": [...]
      }

    Important:
      - ts is UTC epoch seconds, not nanoseconds.
      - Listener validates output column lengths.
    """
    payload = request.get_json(force=True)

    if not isinstance(payload, list):
        return jsonify(error="Payload must be a JSON list of series objects"), 400

    print(f"payload: {payload}")

    # Pick the first non-empty input series as the base timeline for this dummy model.
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
