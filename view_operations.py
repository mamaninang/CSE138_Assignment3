from flask import Flask, request, make_response
import requests
import os
app = Flask(__name__)

key_value_store = {}
view = os.getenv('VIEW')
view_list = view.split(",");
request_address = os.getenv('SOCKET_ADDRESS')

@app.route('/key-value-store-view/<key>', methods=['GET', 'PUT', 'DELETE'])
def view_operations(key):
    #address = 'http://' + ip_address + request.path
    if request.method == 'GET':
        return {"message":"View retrieved succesfully","view":view}, 200

    elif request.method == 'PUT':
        address_to_be_added = os.getenv('socket-address')
        if address_to_be_added in view_list:
            return {"error":"Socket address already exists in the view", "message":"Error in PUT"}, 404
        else:
            view + ',' + address_to_be_added
            
    
    elif request.method == 'DELETE':
        address_to_delete = os.getenv('socket-address')
        #if address_to_delete in view_list:

# def broadcast_Put():


if __name__=='__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)