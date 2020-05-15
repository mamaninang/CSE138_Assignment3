from flask import Flask, request, make_response
import requests
import os
import json
app = Flask(__name__)

headers = {"Content-Type": "application/json"}

@app.route('/key-value-store-view/put', methods=['PUT'])
def broadcast_put():
    if request.method == 'PUT':
        replica_view = os.getenv('VIEW')
        address_to_be_added = request.args['socket-address']
        updated_view = replica_view + "," + address_to_be_added
        os.environ['VIEW'] = updated_view
        
        return {"message":"Replica added successfully to the view"}, 201

@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def view_operations():
    replica_view = os.getenv('VIEW')
    view_list = replica_view.split(",")
    socket_address = os.getenv("SOCKET_ADDRESS")

    if request.method == 'GET':
        return {"message":"View retrieved succesfully","view":replica_view}, 200

    elif request.method == 'PUT':
        address_to_be_added = request.args['socket-address']

        if address_to_be_added in view_list:
            return {"error":"Socket address already exists in the view", "message":"Error in PUT"}, 404

        else:
            updated_view = replica_view + "," + address_to_be_added
            os.environ['VIEW'] = updated_view
            replica_view = os.getenv('VIEW')

            for ip in view_list:
                if ip != socket_address:
                    address_to_search = 'http://' + ip + '/key-value-store-view'
                    response = requests.get(address_to_search, headers=headers)
                    response_json = response.json()
                    response_view = response_json['view']
                    if response_view != replica_view:
                        address_to_search = address_to_search + "/put?socket-address=" + address_to_be_added
                        requests.put(address_to_search, headers=headers, json=request.json)

            return {"message":"Replica added successfully to the view"}, 201
            
    
    elif request.method == 'DELETE':
        address_to_delete = request.args['socket-address']
        if address_to_delete not in view_list:
            return {"error":"Socket address does not exist in the view","message":"Error in DELETE"}, 404
        else:
            view_list.remove(address_to_delete)
            new_env = ",".join(view_list)
            os.environ['VIEW'] = new_env

            # for ip in view:
            #     address_to_search = "http://" + ip + 'key-value-store-view'
            #     checking_ips_view = request.get(address_to_delete, headers=headers)
            #     if address_to_delete in checking_ips_view.view:
            #         request.put(address_to_search, headers=headers, data={'socket-address': address_to_delete})
            return {"message":"Replica deleted successfully from the view"}, 200


if __name__=='__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)