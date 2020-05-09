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
app = Flask(__name__)

key_value_store = {}
socketAddress = os.getenviron("SOCKET_ADDRESS")     
vectorClock = {"10.10.0.2": 0, "10.10.0.3":0, "10.10.0.4":0}

@app.route('/key-value-store/<key>', methods=['GET','PUT','DELETE'])
def kvs(key):

    #ping other replicas
    #vectorClockJson = json.loads(vectorClock)

    if request.method == 'GET':
        return make_response({"doesExist":true,"message":"Retrieved successfully","value":"%s"} % key_value_store[key], 200) \
            if key in key_value_store else make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}', 404)

    if request.method == 'PUT':
        causalMetadata = request.json["causal-metadata"]


        if causalMetadata is None:
            kvs[key] = request.json
            vectorClock[socketAddress] += 1
            checkRequestQueue(key, causalMetadata)
            return make_response(jsonify('{"message":"Added successfully", "causal-metadata": "{vectorClock}"}'), 201)
        else:

            #checks if request in requestQueue has same causal metadata
            if causalMetadata is requestQueue[key]["causal-metadata"]:     #same as checkRequestQueue??
                kvs[key] = requestQueue[key]
                del requestQueue[key]
                vectorClock[socketAddress] += 1
                return make_response(jsonify('{"message":"Added successfully", "causal-metadata": "{vectorClock}"}'), 201)
            else:
                requestQueue[key] = request.json

        #after completing put request, check if any request in requestQueue causally depend on current request
        #checkRequestQueue(key, causalMetadata)

        #call broadcast method

            
    if request.method == 'DELETE':
        if (key in key_value_store.keys()):
            key_value_store.pop(key)
            return make_response('{"doesExist":true,"message":"Deleted successfully"}', 200)
        else: 
            return make_response('{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}', 404)
    
def checkRequestQueue(key, causalMetadata):
    if key in requestQueue and causalMetadata in requestQueue[key]:
        kvs[key] = requestQueue[key]
        del requestQueue[key]
        vectorClock[socketAddress] += 1
        return make_response(jsonify('{"message":"Added successfully", "causal-metadata": "{vectorClock}"}'), 201)
    #elif key in kvs   

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8085)