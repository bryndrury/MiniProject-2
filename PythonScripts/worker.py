# For Communication 
import os
import pika

import HZZ.worker_processing as processing


def callback(ch, method, properties, body):
    # Handled in module
    message_compressed, s, val = processing.process_incomming_request(body)
    
    ch.basic_publish(exchange='', routing_key='result_queue', body=message_compressed)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f" [x] Sent result for job: {s}, {val}")

if __name__ == '__main__':
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    params = pika.ConnectionParameters(
        host = rabbitmq_host,
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue='work_queue')
    channel.queue_declare(queue='result_queue')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='work_queue', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    
    try:
        channel.start_consuming()
        channel.stop_consuming()
    except KeyboardInterrupt:
        print(f" [*] Stopping...")
    finally:
        connection.close()
        print(f" [x] Connection closed.")