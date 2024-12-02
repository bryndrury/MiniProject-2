import os
import time
import pika
    
import HZZ.manager_processing as processing
import HZZ.plotting_function as plotting_function
from HZZ.definitions import samples, fraction, step_size


def send_workload(job_list) -> None:
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    params = pika.ConnectionParameters(rabbitmq_host)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    channel.queue_declare(queue='work_queue')
    channel.queue_declare(queue='result_queue')
    
    processing.publish_jobs(channel, job_list, fraction, step_size)
    
    connection.close()

def receive_results(collected_results, job_list) -> None:
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    params = pika.ConnectionParameters(rabbitmq_host)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    processing.manager_receive_results(channel, collected_results, job_list)
    
    channel.start_consuming()
    connection.close()

if __name__ == "__main__":
    collected_results = []
    job_list = ["STOPPER"]
    processing.calculate_workload(job_list)
    
    processing_time = time.time()
    send_workload(job_list)
    receive_results(collected_results, job_list)
    print(f"Processing time: {time.time() - processing_time}")
    
    print(f"Reformatting results...", end="\r")
    all_data = processing.reformat_results(collected_results, samples)
    print(f"Results reformatted.   ", end="\n")
    
    print(f"Producing plot...      ", end="\r")
    volume_path = "/usr/src/app/data"
    plotting_function.plot(all_data, samples, fraction, volume_path)
    print(f"Plot produced.         ", end="\n")