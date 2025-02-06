import json
from kafka import KafkaProducer


# Função para criar uma conexão com o Kafka
def create_kafka_producer(bootstrap_servers='kafka:9092'):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Conexão com o Kafka estabelecida com sucesso.")
        return producer
    except Exception as e:
        print(f"Erro ao conectar com o Kafka: {e}")
        raise

# Função para enviar dados para o Kafka
def send_to_kafka(producer, topic, data):
    try:
        producer.send(topic, data)
        producer.flush()
        print(f"Dados enviados para o tópico {topic}: {data}")
    except Exception as e:
        print(f"Erro ao enviar dados para o Kafka: {e}")
        raise