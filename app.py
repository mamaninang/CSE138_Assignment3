####################
# Course: CSE138
# Date: Spring 2020
# Author: Alan Vasilkovsky, Mariah Maninang, Bradley Gallardo, Omar Quinones
# Assignment: 2
# Description: 
###################

from flask import Flask, request, make_response, jsonify
import os
import requests
import json
app = Flask(__name__)

key_value_store = {}
ip, port = os.getenv('SOCKET_ADDRESS').split(':')[0], os.getenv('SOCKET_ADDRESS').split(':')[1]
vectorClock = {"10.10.0.2": 0, "10.10.0.3":0, "10.10.0.4":0}
requestQueue = {}

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):

    #ping other replicas

    if request.method == 'GET':
        return make_response('{"doesExist":true,"message":"Retrieved successfully","value":"%s"}' % key_value_store[key], 200) \
            if key in key_value_store else make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}', 404)

    if request.method == 'PUT':
        causalMetadata = json.loads(json.dumps(request.json["causal-metadata"]))

        #if no causal metadata, then complete request right away
        if causalMetadata == "" or vectorClock == causalMetadata:
            key_value_store[key] = request.json
            vectorClock[ip] += 1
            #checkRequestQueue(key, causalMetadata) 
            #call broadcast method

            return make_response('{"message":"Added successfully", "causal-metadata": "%s"}' % vectorClock, 201)

        #if there is metadata, do the following
        else:
            requestQueue[key] = request.json
            return make_response('{"message": "Request placed in a queue", "request": "%s"}' % requestQueue[key], 201)

            
    if request.method == 'DELETE':
        if (key in key_value_store.keys()):
            key_value_store.pop(key)
            return make_response('{"doesExist":true,"message":"Deleted successfully"}', 200)
        else: 
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}', 404)
    
def checkRequestQueue():
    address = "http://" + ip + port + "/key-value-store/"
    headers = {"Content-Type": "application/json"}

    if request.method == 'PUT':
        for key in requestQueue:
            if requestQueue[key]["causal-metadata"] == vectorClock:
                address += key
                value = requestQueue[key]
    
        return requests.put(address, headers=headers, json=value)


def isLessOrSameVC(causalMetadata):
    for key in vectorClock:
        if causalMetadata[key] > vectorClock[key]:
            return False
    return True

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)