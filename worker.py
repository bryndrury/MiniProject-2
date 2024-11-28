# For Communication 
import os
import pika
import json
import zlib

# For Comutation
import infofile
import vector
import time
import uproot
import awkward as ak


# ================================================== #
# Definitions:                                       #
# ================================================== #
# Define constants
MeV = 0.001
GeV = 1.0
# Set luminosity to 10 fb-1 for all data
lumi = 10
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

# ================================================== #
# The Work Functions:                                #
# ================================================== #
# Cut lepton type (electron type is 11,  muon type is 13)
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool # True means we should remove this entry (lepton type does not match)

# Cut lepton charge
def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge # True means we should remove this entry (sum of lepton charges is not equal to 0)

# Calculate invariant mass of the 4-lepton state, [:, i] selects the i-th lepton in each event
def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV # .M calculates the invariant mass
    return invariant_mass

def calc_weight(weight_variables, sample, events):
    info = infofile.infos[sample]
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    total_weight = xsec_weight 
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

def work_on_data(val, data, sample_data, start, download_time) -> None:
    # Number of events in this batch
    nIn = len(data) 
                            
    # Record transverse momenta (see bonus activity for explanation)
    data['leading_lep_pt'] = data['lep_pt'][:,0]
    data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
    data['third_leading_lep_pt'] = data['lep_pt'][:,2]
    data['last_lep_pt'] = data['lep_pt'][:,3]

    # Cuts
    lep_type = data['lep_type']
    data = data[~cut_lep_type(lep_type)]
    lep_charge = data['lep_charge']
    data = data[~cut_lep_charge(lep_charge)]
    
    # Invariant Mass
    data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

    # Store Monte Carlo weights in the data
    if 'data' not in val: # Only calculates weights if the data is MC
        data['totalWeight'] = calc_weight(weight_variables, val, data)
        nOut = sum(data['totalWeight']) # sum of weights passing cuts in this batch 
    else:
        nOut = len(data)
    elapsed = time.time() - start # time taken to process
    # print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)+"\t in "+str(round(elapsed,1))+"s") # events before and after
    # print("\t\t Compute time: " + str(round(elapsed - download_time,1))+"s," + 
            # "\tPercentage of total time: " + str(round((elapsed - download_time)/elapsed*100,1)) + "%")

    # Append data to the whole sample data list
    sample_data.append(data)

def work_on_file(s, val, fraction, step_size) -> ak.Array:
    if s == 'data': 
        prefix = "Data/" # Data prefix
    else: # MC prefix
        prefix = "MC/mc_"+str(infofile.infos[val]["DSID"])+"."
    fileString = path+prefix+val+".4lep.root" # file name to open
    
    # start the clock
    start = time.time() 
    # print("\t"+val+":") 

    # Open file (and time the download)
    download_time_start = time.time()
    tree = uproot.open(fileString + ":mini")
    download_time = time.time() - download_time_start
    # print("\t\t Time to download: "+str(round(download_time,1))+"s")
    
    sample_data = []

    # print(f"\t\t Number of entries in tree of value: {tree.num_entries}\n")
    for data in tree.iterate(variables + weight_variables, 
                            library="ak", 
                            entry_stop=tree.num_entries*fraction, # process up to numevents*fraction
                            step_size = step_size): 
        work_on_data(val, data, sample_data, start, download_time)

    tree.close() # Ensure the file it closed
    return ak.concatenate(sample_data)


# ================================================== #
# The Communication Function:                        #
# ================================================== #
def callback(ch, method, properties, body):
    message = json.loads(body)
    s = message['s']
    val = message['val']
    fraction = message['fraction']
    step_size = message['step_size']
    
    print(f"\n [x] Received job: {s}, {val}", end="\r")

    print(f" [*] Working on job: {s}, {val}", end="\r")
    result = work_on_file(s, val, fraction, step_size)
    print(f" [x] Finished job: {s}, {val}  ", end="\n")
    
    
    result_message = {"s": s, 
                      "val": val,
                      "result": result.tolist()
    }
    
    result_message = json.dumps(result_message) # Json-ify the message to send
    print(f" [*] Compressing result, uncompressed size: {len(result_message)}  ", end="\r")
    
    message_compressed = zlib.compress(result_message.encode('utf-8'), level=9) # Compress the message
    print(f" [x] Compressed result, compressed size: {len(result_message)}     ", end="\n")
    
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