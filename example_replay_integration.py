# Add 'replay_endpoint = "http://localhost:5000"' to the [server] table in the HQM server config

from flask import Flask, request

app = Flask(__name__)

@app.route('/', methods=['POST'])
def result():
    time = request.form["time"]
    server_name = request.form["server"]
    replay_data = request.files["replay"].read() # Same as .hrp data
    return ""
