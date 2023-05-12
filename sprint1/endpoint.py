from flask import Flask
from flask import request
from FullyAutomated import main

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def FA():
    return main(request.json)