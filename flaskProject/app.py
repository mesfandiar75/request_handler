from flask import Flask, jsonify
import multiprocessing
import dataset
import time

app = Flask(__name__)
DATABASE_URL = 'sqlite:///dev.db'


def update_request(request_status, request_id):
    db = dataset.connect(DATABASE_URL)
    # if request_status is "run" the request that status was "queue" change to "run"
    if request_status == 'run':
        requests = db.query(f'''select * from request
                                    where request_status = "queue"
                                    order by request_weight asc, request_id asc''')
        requests_dict = jsonify({'result': [dict(row) for row in requests]}).json
        if requests_dict['result']:
            update_data = {'request_id': request_id, 'request_status': request_status}
            db['request'].update(update_data, ['request_id'])

    # if request_status is "finished" the request that status was "run" change to "finished"
    elif request_status == 'finished':
        requests = db.query(f'''select * from request
                                where request_status = "run"
                                order by request_weight asc, request_id asc''')
        requests_dict = jsonify({'result': [dict(row) for row in requests]}).json
        if requests_dict['result']:
            update_data = {'request_id': request_id, 'request_status': request_status}
            db['request'].update(update_data, ['request_id'])
    return True


def limited_f(x, request_id):
    # change request status to "run"
    thread_update = multiprocessing.Process(target=update_request, args=('run', request_id))
    thread_update.start()
    time.sleep(10)

    # do some thing with x

    # change request status to "finished"
    thread = multiprocessing.Process(target=update_request, args=('finished', request_id))
    thread.start()

    # call request handler for continue the process
    thread_new_request = multiprocessing.Process(target=request_handler)
    thread_new_request.start()
    return True


def request_handler():
    db = dataset.connect(DATABASE_URL)

    # select the request that having priority according to the requests weight
    requests = db.query(f'''select * from request
                            where request_status = "queue" or request_status = "run"
                            order by request_weight asc, request_id asc''')
    requests_dict = jsonify({'result': [dict(row) for row in requests]}).json

    # call limited_f
    if requests_dict['result']:
        limited_f(requests_dict['result'][0]['x'], requests_dict['result'][0]['request_id'])

    return True


@app.route('/request/<user_id>,<x>')
def request(user_id, x):
    db = dataset.connect(DATABASE_URL)
    # default request_id
    request_id = 1

    # set the weight for request according to the user last requests
    request_weight_query = db.query(f'''select count(request_id) as weight from request where user_id={user_id}''')
    response = jsonify({'result': [dict(row) for row in request_weight_query]})
    request_weight = response.json['result'][0]['weight'] + 1

    # max request id plus one for new request
    user_last_request = db.query(f'''select max(request_id) as max_id from request''')
    user_last_request = jsonify({'result': [dict(row) for row in user_last_request]}).json
    max_id = user_last_request['result'][0]['max_id']

    if max_id:
        request_id = max_id + 1
        update_data = {'user_id': user_id, 'request_weight': request_weight}
        db['request'].update(update_data, ['user_id'])

    # insert new request with status "queue"
    user_request = {'request_id': request_id, 'user_id': user_id, 'request_status': 'queue',
                    'request_weight': request_weight, 'x': x}
    db['request'].insert(user_request)

    # call request handler
    thread_new_request = multiprocessing.Process(target=request_handler)
    thread_new_request.start()

    return jsonify(user_request)


if __name__ == '__main__':
    app.run(debug=True)
