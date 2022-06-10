import subprocess
import shutil
import os
import sys

# python3 server/develop/deploy.py getRawDataBitFlyer getData
# python3 server/develop/deploy.py getRawDataBitFlyerBack getDataBack

args = sys.argv
FUNC_NAME = args[1]
METHOD_NAME = args[2]

# FUNC_NAME = "getRawDataBitFlyer"
# METHOD_NAME = "getData"

# FUNC_NAME = "getRawDataBitFlyerBack"
# METHOD_NAME = "getDataBack"

PGM_NAME = "getData.py"
DIR = "server/develop"
MEMORY = "256MB"
CREATE_PPUBSUB = False
CREATE_JOB = False
SCHEDULE = "0 */3 * * *"


os.chdir(DIR)
if CREATE_PPUBSUB:
    cmd = [
        "gcloud", "pubsub", "topics",
        "create", FUNC_NAME
    ]
    cp = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE)

if not os.path.exists(f"../operate/{FUNC_NAME}"):
    os.makedirs(f"../operate/{FUNC_NAME}")

shutil.copy2(PGM_NAME, "../operate/{}/main.py".format(FUNC_NAME))
shutil.copy2("requirements.txt", "../operate/{}/requirements.txt".format(FUNC_NAME))
shutil.copy2("serviceAccount.json", "../operate/{}/serviceAccount.json".format(FUNC_NAME))

os.chdir(f"../operate/{FUNC_NAME}/")

cmd = [
    "gcloud", "functions", "deploy", FUNC_NAME,
    f"--entry-point={METHOD_NAME}",
    f"--region=asia-northeast1",
    f"--runtime=python38",
    f"--memory={MEMORY}",
    f"--trigger-topic={FUNC_NAME}",
    f"--timeout=540",
]
cp = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE)


if CREATE_JOB:
    cmd = [
        "gcloud", "scheduler", "jobs", "create", "pubsub", FUNC_NAME,
        f'--schedule={SCHEDULE}',
        f"--topic=cron-topic",
        f'--message-body="{FUNC_NAME}"'
    ]
    cp = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE)
