# sds_de test




As we need to install some libraries / jars i do created two dockerfiles - which one is responsable for the Airflow + java package to connect to spark, and the second one is the spark image with the jar to connect to kafka - all the rest are just pure images from the docker repository (postgres, kafka, zookeper)

Well, i changed a little bit how everything works - so instead of using the whole metadata at once , i split it in two steps

- First i simulated with a mock, an API call to bring the JSON with the people data - Which is the closest i could to to look like a real pipeline, without the need to build a real API.
- In the second step, we are using a JSON with the transformations steps (validation and transformation)
- in the third but no least important, we are using just the "sinks" piece of the json , to send data across places.

TO store those jsons, i put some airflow variables - and they are exported here in this repo as "airflow_variables.json".

OBS: I also put a small task in the dag (first task) to validate if your connection with spark is working - so any problem, check docker compose or the spark_default connection on airflow.


## Well, lets begin on how to run:

>Certify that you have docker in his last update, done your login and clone the repository.

As our dockerfiles and compose are in the root of the repository, you just need to run `docker-compose up --build --force-recreate` and then you are going to have this containers:

![Containers on docker desktop](/images/containers.jpg)


Our containers are already creating an airflow user - login `admin` and password `admin` - so its time to access our airflow in the http://localhost:8080/ in the navigator of your choice.

To be able to run without any issues, import to airflow the  [airflow variables](airflow_variables.json), and configura the spark_default connection to look like this:

![spark_default conn config](/images/spark_default_conn.jpg)

After configuring the conn and the variables, all you need to do is turn on the DAG and press into the Trigger run button.

## Results
You can check the resuts after, taking a look at the hadoop datanode and kafka containers.

Example: 

> 
    kafka-topics --bootstrap-server kafka:9092 --list

> 
    kafka-console-consumer --bootstrap-server kafka:9092 --topic person --from-beginning

You are going to find this result:

![spark_default conn config](/images/kafk_result.jpg)

## What could we improve?

Well, since i had some problems while doing the test, i decided to take some shortcuts to deliver in time and running, so here are the technical debts that i assume on this project:

- Better separation of responsabilities on tasks 
- Create another path for python codes, and not use same one as dags
- Create a better documentation
- Create tests
- Create a visualization page so we could see all the results in the same spot
- airflow could be separated (scheduler, webservice, use different executors)
- FIx version of images on docker compose / requirements
  
Thank you so much!

Best regards,

Giovane