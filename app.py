from flask import Flask, request
import requests

app = Flask(__name__)

view = ["10.10.0.2:8085","10.10.0.3:8085","10.10.0.4:8085","localhost:8085"]

def backup(request, json=None):
    global view
    new_view = []

    for ip in view:
        # Perform a GET request to every IP address in the current view
        try:
            result = requests.get('http://{}/status'.format(ip), timeout=1)
            
            if result.text == 'alive' and result.status_code == 200:
                new_view.append(ip)
        
        except requests.exceptions.Timeout:
            print("ip {} timed out".format(ip))
            continue
    
    view = new_view
    print("new view: {}".format(view))

    if request == 'GET':
        for ip in view:
            requests.get('http://{}/key-value-store')
        return

    if request == 'PUT' and json is not None:
        for ip in view:
            requests.put('http://{}/key-value-store', json=json)
        return

    if request == 'DELETE':
        for ip in view:
            requests.delete('http://{}/key-value-store')
        return
    
    print("Invalid backup request")
    

@app.route('/status', methods=['GET'])
def status():
    return 'alive', 200

@app.route('/main', methods=['GET'])
def main():
    backup('GET')
    return 'done', 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8085)