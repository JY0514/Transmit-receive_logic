from flask import Flask, request, json, jsonify
import json

app = Flask(__name__)

@app.route("/send", methods=['POST'])
def versionCheck():

    response = {
        "result": "ok"
    }
    return jsonify(response)



