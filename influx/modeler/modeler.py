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
    # Convert to string, keep only digits
    digits = "".join(ch for ch in str(x) if ch.isdigit())

    if not digits:
        return 0

    return int(digits[:3])


@app.route("/model", methods=["POST"])
def model():
    payload = request.get_json(force=True)

    if not isinstance(payload, dict):
        return jsonify(error="Payload must be a JSON object of key -> list"), 400

    print(f"payload: {payload}")

    out_dic = {}

    for k, vlist in payload.items():
        # Basic validation: each value should be a list
        if not isinstance(vlist, list):
            return jsonify(error=f"Value for key '{k}' must be a list"), 400

        # If list is empty, just return empty list
        if len(vlist) == 0:
            out_dic[k] = []
            continue

        # 1. Keep ts exactly as it is
        if k == "ts":
            out_dic[k] = vlist

        # 2. Strings -> add "_m" to each string
        elif isinstance(vlist[0], str):
            out_dic[k] = [f"{item}_m" for item in vlist]

        # 3. Numbers (int or float) -> keep first 3 digits
        elif isinstance(vlist[0], (int, float)):
            out_dic[k] = [first_3_digits_as_int(item) for item in vlist]

        # 4. Anything else -> return as is
        else:
            out_dic[k] = vlist

    print(f"out_dic: {out_dic}")

    return jsonify(out_dic)


if __name__ == "__main__":
    # Runs on 0.0.0.0:8080 to match your listener flags
    app.run(host="0.0.0.0", port=8080)
