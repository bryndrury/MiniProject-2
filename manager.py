import os
import json
import zlib
import pika
import awkward as ak
    
import plot_function

# Define constants
MeV = 0.001
GeV = 1.0
# Set luminosity to 10 fb-1 for all data
lumi = 10
# Controls the fraction of all events analysed
fraction = 1.0 # reduce this is if you want quicker runtime (implemented in the loop over the tree)
step_size = 1000000
# Define empty dictionary to hold awkward arrays
all_data = {}
# Path to the data
path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
# For identification and naming
samples = {
    'data': {
        'list' : ['data_A','data_B','data_C','data_D'], # data is from 2016, first four periods of data taking (ABCD)
    },
    r'Background $Z,t\bar{t}$' : { # Z + ttbar
        'list' : ['Zee','Zmumu','ttbar_lep'],
        'color' : "#6b59d3" # purple
    },
    r'Background $ZZ^*$' : { # ZZ
        'list' : ['llll'],
        'color' : "#ff0000" # red
    },
    r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
        'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
        'color' : "#00cdff" # light blue
        },
}
variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']
weight_variables = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]


def calculate_workload(job_list: list) -> None:
    # Append all jobs to the job list
    for s in samples:
        for val in samples[s]['list']:
            job = (s, val)
            job_list.append(job)

def send_workload(job_list) -> None:
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    params = pika.ConnectionParameters(rabbitmq_host)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    channel.queue_declare(queue='work_queue')
    channel.queue_declare(queue='result_queue')
    
    for job in job_list:
        if job == "STOPPER":
            continue
        
        s = job[0]
        val = job[1]
        
        message = json.dumps({"s": s, 
                            "val": val, 
                            "fraction": fraction, 
                            "step_size": step_size})
        channel.basic_publish(exchange='', routing_key='work_queue', body=message)
        print(f" [x] Sent {message}")
        
    connection.close()

def receive_results(collected_results) -> None:
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    params = pika.ConnectionParameters(
        host = rabbitmq_host,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    def callback(ch, method, properties, body):
        # Decompress the message
        decompressed_message = zlib.decompress(body).decode('utf-8')
        
        # Decode message
        message = json.loads(decompressed_message)
        
        # Extract the information
        s = message['s']
        val = message['val']
        
        # Confirm receipt
        print(f" [x] Received result for job: {s}, {val}. {len(job_list)-2} jobs remaining.")
        
        # Append the result to the list
        collected_results.append(message)
        
        # search for the job in the list
        for job in job_list:
            if job[0] == s and job[1] == val:
                job_list.remove(job)
                break
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        if len(job_list) == 1 and job_list[0] == "STOPPER":
            print(f"\n [x] All results received.")
            ch.stop_consuming()
            
            # ============================================================================== #
            # Optional: Uncomment to delete the work queue to stop the workers once finished #
            
            # ch.queue_delete(queue='work_queue') 
            ch.close()
            return
        
    # Set up the consumer
    channel.basic_consume(queue='result_queue', on_message_callback=callback)
    print(f' [*] Waiting for {len(job_list)} results. To exit press CTRL+C', end="\n\n")
    
    channel.start_consuming()
    connection.close()
    
    
def reformat_results(collected_results: dict) -> dict:
    all_data = {}
    
    # Loop over samples to gather the results for each sample
    for s in samples:
        # All dictionaries that has the first key as s
        frame_data = [result for result in collected_results if result['s'] == s]
        frames = []
        # Loop through the frame data and append the values to the list
        for data in frame_data:
            frames.append(data['result'])

        all_data[s] = ak.concatenate(frames) # dictionary entry is concatenated awkward arrays

    collected_results.clear() # Clear the list of results to free up memory
    return all_data

if __name__ == "__main__":
    collected_results = []
    job_list = ["STOPPER"]
    calculate_workload(job_list)
    
    send_workload(job_list)
    receive_results(collected_results)
    
    print(f"Reformatting results...", end="\r")
    all_data = reformat_results(collected_results)
    print(f"Results reformatted.   ", end="\r")
    
    print(f"Producing plot...      ", end="\r")
    volume_path = "/usr/src/app/data"
    plot_function.plot(all_data, samples, fraction, step_size, volume_path)
    print(f"Plot produced.         ", end="\r")