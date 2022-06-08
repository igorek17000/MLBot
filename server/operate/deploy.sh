
PGM_NAME=getData.py
FUNC_NAME=getRawDataBitFlyer
METHOD_NAME=getData
MEMORY=128MB

cp ../develop/${PGM_NAME} ./main.py
cp ../develop/requirements.txt ./requirements.txt
cp ../develop/serviceAccount.json ./serviceAccount.json


gcloud pubsub topics create ${FUNC_NAME}

gcloud functions deploy ${FUNC_NAME} \
--entry-point=${METHOD_NAME} \
--region=asia-northeast1 \
--runtime=python38 \
--memory=${MEMORY} \
--trigger-topic=projects/mlbot-352401/topics/${FUNC_NAME} \
--timeout=540

