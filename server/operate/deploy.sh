
PGM_NAME=getData.py
FUNC_NAME=getRawDataBitFlyer
METHOD_NAME=getData
MEMORY=256MB

gcloud pubsub topics create ${FUNC_NAME}


cp ../develop/${PGM_NAME} ./${FUNC_NAME}/main.py
cp ../develop/requirements.txt ./${FUNC_NAME}/requirements.txt
cp ../develop/serviceAccount.json ./${FUNC_NAME}/serviceAccount.json

cd ./${FUNC_NAME}/

gcloud functions deploy ${FUNC_NAME} \
--entry-point=${METHOD_NAME} \
--region=asia-northeast1 \
--runtime=python38 \
--memory=${MEMORY} \
--trigger-topic=projects/mlbot-352401/topics/${FUNC_NAME} \
--timeout=540

