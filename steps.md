https://github.com/puckel/docker-airflow

#Start LocalExecutor :

docker-compose -f docker-compose-LocalExecutor.yml up -d

Turn off Local Executor:

docker-compose -f docker-compose-LocalExecutor.yml down 

###checking containers###

docker ps

###killing container###

docker container kill [container id]

###loging in container via bash###

docker exec -it <container name> bash

```
e.g docker exec -it docker-airflow_webserver_1 bash
```

###loggin in container via bin/bash###

docker exec -t -i <container name> /bin/bash

e.g

docker exec -t -i docker-airflow_webserver_1 /bin/bash

###To change dags folder go to docker-compose-LocalExecutor.yml file and change the volume part to the folder you want:###
volumes:
            - /Users/konradburchardt/airflow/dags:/usr/local/airflow/dags


### Installing Python libraries inside docker ###

python -m pip install --user <library name>

### Connecting to s3 via Hook ###

Creagte a new connection in Admin >> Connections:

Conn Id: my_conn_S3

Conn Type: S3

Extra: {"aws_access_key_id":"_your_aws_access_key_id_", "aws_secret_access_key": "_your_aws_secret_access_key_"}

 



	






