from time import sleep
import requests
import os
from firebase_admin import firestore
from firebase_admin import credentials
import firebase_admin
from warnings import warn

# os.chdir('server/develop/')


def getData(event, context):
    getBitFlyer()


def getDataBack(event, context):
    getBitFlyerBack()


def getBitFlyer():
    cred = credentials.Certificate('./serviceAccount.json')
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    doc_ref = (
        db
        .collection('Exchanger')
        .document('bitFlyer')
    )

    endpoint = "https://api.bitflyer.com/v1/"

    res = doc_ref.get().to_dict()
    start_id = int(res["latest_id"])
    while True:
        pyaload = {"product_code": "BTC_JPY", "count": 500, "after": start_id}

        res_dict_list = requests.get(endpoint + "executions", params=pyaload).json()
        # dir(res)

        if len(res_dict_list) == 0:
            warn("過去訴求は限界")
            break

        new_start_dict = res_dict_list[0]
        latest_id = new_start_dict["id"]
        latest_dt = new_start_dict["exec_date"][:10].replace('-', '')

        print(latest_id, latest_dt)

        for res_dict in res_dict_list:
            row = doc_ref.collection('raw_data').document(str(res_dict["id"]))
            row.set(res_dict)

        doc_ref.update({"latest_ymd": latest_dt, "latest_id": latest_id})

        start_id = latest_id
        sleep(0.8)


def getBitFlyerBack():
    cred = credentials.Certificate('./serviceAccount.json')
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    doc_ref = (
        db
        .collection('Exchanger')
        .document('bitFlyer')
    )

    endpoint = "https://api.bitflyer.com/v1/"

    res = doc_ref.get().to_dict()
    start_id = int(res["oldest_id"])
    for i in range(15):
        pyaload = {"product_code": "BTC_JPY", "count": 500, "before": start_id}

        res_dict_list = requests.get(endpoint + "executions", params=pyaload).json()
        # dir(res)

        if len(res_dict_list) == 0:
            break

        new_start_dict = res_dict_list[-1]
        oldest_id = new_start_dict["id"]
        oldest_dt = new_start_dict["exec_date"][:10].replace('-', '')

        print(oldest_id, oldest_dt)

        for res_dict in res_dict_list:
            row = doc_ref.collection('raw_data').document(str(res_dict["id"]))
            row.set(res_dict)

        doc_ref.update({"oldest_ymd": oldest_dt, "oldest_id": oldest_id})

        start_id = oldest_id
        sleep(0.8)
