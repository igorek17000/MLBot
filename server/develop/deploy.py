import subprocess
import shutil
import os
import sys

# python3 server/develop/deploy.py getRawDataBitFlyer getData
# python3 server/develop/deploy.py getRawDataBitFlyerBack getDataBack
# python3 server/develop/deploy.py processingDataBitFlyerDoll300M makeBar processingData.py

args = sys.argv
FUNC_NAME = args[1]
METHOD_NAME = args[2]
PGM_NAME = args[3]
CREATE_PPUBSUB = False
CREATE_JOB = False

# FUNC_NAME = "getRawDataBitFlyer"
# METHOD_NAME = "getData"

# FUNC_NAME = "getRawDataBitFlyerBack"
# METHOD_NAME = "getDataBack"

# PGM_NAME = "processingData.py"
# FUNC_NAME = "processingDataBitFlyerDoll300M"
# METHOD_NAME = "makeBar"

PGM_NAME = "realtimeTrade.py"
REQUIREMENTS_NAME = "requirements_judge.txt"
FUNC_NAME = "realtimeTradeBitFlyerDoll300M"
METHOD_NAME = "trade"
TRRIGER_TYPE = "FireStore"
RESOURCE_PATH = "Exchanger/bitFlyer/processing_doll_bar_300000000/{doc1}"


DIR = "server/develop"
MEMORY = "256MB"
SCHEDULE = "*/13 * * * *"


os.chdir(DIR)
print(os.getcwd())
if CREATE_PPUBSUB:
    cmd = [
        "gcloud", "pubsub", "topics",
        "create", FUNC_NAME
    ]
    cp = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE)

if not os.path.exists(f"../operate/{FUNC_NAME}"):
    os.makedirs(f"../operate/{FUNC_NAME}")

shutil.copy2(PGM_NAME, "../operate/{}/main.py".format(FUNC_NAME))
shutil.copy2(REQUIREMENTS_NAME, "../operate/{}/requirements.txt".format(FUNC_NAME))
shutil.copy2("serviceAccount.json", "../operate/{}/serviceAccount.json".format(FUNC_NAME))

os.chdir(f"../operate/{FUNC_NAME}/")
print(os.getcwd())

if TRRIGER_TYPE == "PUBSUB":
    cmd = [
        "gcloud", "functions", "deploy", FUNC_NAME,
        f"--entry-point={METHOD_NAME}",
        f"--region=asia-northeast1",
        f"--runtime=python38",
        f"--memory={MEMORY}",
        f"--trigger-topic={FUNC_NAME}",
        f"--timeout=540",
    ]
elif TRRIGER_TYPE == "FireStore":
    cmd = [
        "gcloud", "functions", "deploy", FUNC_NAME,
        f"--entry-point={METHOD_NAME}",
        f"--region=asia-northeast1",
        f"--runtime=python38",
        f"--memory={MEMORY}",
        # f"--trigger-topic={FUNC_NAME}",
        f'--trigger-event=providers/cloud.firestore/eventTypes/document.create',
        f'--trigger-resource=projects/mlbot-352401/databases/(default)/documents/{RESOURCE_PATH}',
        f"--timeout=540",
        f'--set-env-vars=BIT_FLYER_API_KEY={os.environ["BIT_FLYER_API_KEY"]},BIT_FLYER_API_SECRET={os.environ["BIT_FLYER_API_SECRET"]}'
    ]
cp = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE)


if CREATE_JOB:
    cmd = [
        "gcloud", "scheduler", "jobs", "create", "pubsub", FUNC_NAME,
        f'--schedule={SCHEDULE}',
        f"--topic={FUNC_NAME}",
        f'--message-body="{FUNC_NAME}"',
    ]
    cp = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE)
