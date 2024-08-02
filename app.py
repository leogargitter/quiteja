from extract_data import extract_types_dict
import os
from flask import Flask, jsonify

app = Flask(__name__)

path_to_data = os.path.join(os.getcwd(), "dados.zip")
types = extract_types_dict(path_to_data, "tipos.csv")


@app.route("/tipo/<int:id>", methods=["GET"])
def get_type_from_id(id):
    try:
        key = "tipo"
        value = types[id]
        status = 200
    except KeyError:
        key = "erro"
        value = "Tipo não encontrado"
        status = 404

    return jsonify({key: value}), status
