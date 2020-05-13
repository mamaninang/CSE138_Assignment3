from flask import Flask, request, make_response
import requests
import os
app = Flask(__name__)

# view = os.getenv('VIEW')
# view_list = view.split(",");
view = ["10.10.0.2:8085","10.10.0.3:8085","10.10.0.4:8085"]
request_address = os.getenv('SOCKET_ADDRESS')
headers = {"Content-Type": "application/json"}

@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def view_operations():
    if request_address is None:
        if request.method == 'GET':
            return {"message":"View retrieved succesfully",'view':view}, 200

        elif request.method == 'PUT':
            address_to_be_added = request.json["socket-address"]
            if address_to_be_added in view:
                return {"error":"Socket address already exists in the view", "message":"Error in PUT"}, 404
            else:
                view.append(',' + address_to_be_added)

                for ip in view:
                    address_to_search = 'http://' + ip + '/key-value-store-view'
                    checking_ips_view = requests.get(address_to_search, headers=headers)
                    if address_to_be_added not in checking_ips_view.view:
                        request.put(address_to_search, headers=headers, data={"socket-address":address_to_be_added})

                return {"message":"Replica added successfully to the view"}, 201
                
        
        elif request.method == 'DELETE':
            address_to_delete = request.json['socket-address']
            if address_to_delete not in view:
                return {"error":"Socket address does not exist in the view","message":"Error in DELETE"}, 404
            else:
                view.remove(address_to_delete)

                for ip in view:
                    address_to_search = "http://" + ip + 'key-value-store-view'
                    checking_ips_view = request.get(address_to_delete, headers=headers)
                    if address_to_delete in checking_ips_view.view:
                        request.put(address_to_search, headers=headers, data={'socket-address': address_to_delete})
                return {"message":"Replica deleted successfully from the view"}, 200

    else:
        response_address = 'http://' + request_address + "/key-value-store-view"

        if request.method == 'GET':
            return requests.get(response_address, headers=headers)
        
        elif request.method == 'PUT':
            return request.put(request_address, headers=headers)

        elif request.method == 'DELETE':
            return request.delete(request_address, headers=headers)

if __name__=='__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)