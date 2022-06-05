git clone https://github.com/Gaborotta/MLBot.git

apt-get update
apt-get install -y python3 python3-pip

echo 'alias python=python3' >> ~/.bashrc

pip install pandas
pip install requestsRUN apt-get update
apt-get install -y python3 python3-pip

echo 'alias python=python3' >> ~/.bashrc

pip install pandas
pip install requests

echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc

AIRFLOW_VERSION=2.3.2
PYTHON_VERSION=3.8
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

airflow db init
airflow users  create --role Admin --username ${USER_NAE} --email admin --firstname admin --lastname admin --password ${USER_PASS}

# airflow standalone