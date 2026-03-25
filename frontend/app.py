import os
from flask import Flask, jsonify

app = Flask(__name__)

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")


@app.get("/healthcheck")
def healthcheck():
    return jsonify({"status": "ok", "service": "metaminer-frontend"})


@app.get("/")
def index():
    return jsonify({
        "service": "metaminer-frontend",
        "message": "UI coming soon. Use the API at the configured API_BASE_URL.",
        "api": API_BASE_URL,
    })
