# download airflow yaml file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml'

# setup user
echo Enter Machine User Name
read name

# creating directory for airflow that stores the airflow configuration
mkdir /mnt/c/Users/$name/docker
mkdir /mnt/c/Users/$name/docker/airflow

mkdir /mnt/c/Users/$name/docker/airflow/dags
mkdir /mnt/c/Users/$name/docker/airflow/logs
mkdir /mnt/c/Users/$name/docker/airflow/plugins

# move the yaml file
mv docker-compose.yaml /mnt/c/Users/$name/docker/airflow

# start docker
cd /mnt/c/Users/$name/docker/airflow

docker-compose up airflow-init

docker-compose up