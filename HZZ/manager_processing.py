import json
import zlib
import awkward as ak

from HZZ.definitions import samples

# ============================================================================== #
# For Manager:

def calculate_workload(job_list: list) -> None:
    # Append all jobs to the job list
    for s in samples:
        for val in samples[s]['list']:
            job = (s, val)
            job_list.append(job)
            
def publish_jobs(channel, job_list, fraction, step_size):
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
            
def manager_receive_results(channel, collected_results, job_list):
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
                ch.queue_delete(queue='work_queue') 
                ch.queue_delete(queue='result_queue')
                
                ch.close()
                return
            
        # Set up the consumer
        channel.basic_consume(queue='result_queue', on_message_callback=callback)
        print(f' [*] Waiting for {len(job_list)} results. To exit press CTRL+C', end="\n\n")
                    

def reformat_results(collected_results, samples):
    if collected_results == []:
        print("No results to reformat.")
        return {}
    
    all_data = {}
    
    # Loop over samples to gather the results for each sample
    for s in samples:
        frames = []
        for val in samples[s]['list']:
            # Find the result for the sample
            result = next((result for result in collected_results if result['s'] == s and result['val'] == val), None)
            frames.append(ak.Array(result['result']))
        if frames:
            all_data[s] = ak.concatenate(frames)
        else:
            all_data[s] = ak.Array([])

    collected_results.clear() # Clear the list of results to free up memory
    return all_data