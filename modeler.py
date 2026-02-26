from flask import Flask, request, jsonify
import os
import random


app = Flask(__name__)

# Input ts column: ts
# Output ts column: "ts_pred"
# payload: {'device_id': ['dev-1'], 'value': [13.64]}
# out_dic: {'device_id': ['dev-1'], 'value_pred': [20.64]}

@app.route("/model", methods=["POST"])
def model():

    payload = request.get_json(force=True)
    if not isinstance(payload, dict):
        return jsonify(error="Payload must be a JSON object of column_name -> list"), 400

    print(f'payload: {payload}')

    out_dic = {}
    for k, l in payload.items():
        if isinstance(l[0], str):
            out_dic[k] = l
            continue
        if k != "ts":
            out_dic[k + '_pred'] = []
            for v in l:
                out_dic[k + '_pred'].append(v + random.randint(3, 9))


    # out_dic["ts_pred"] = [tsi + 5 for tsi in payload["ts"]]

    print(f'out_dic: {out_dic}')

    return jsonify(out_dic)

if __name__ == "__main__":
    # Runs on 0.0.0.0:8080 to match your listener flags
    app.run(host="0.0.0.0", port=8080)
