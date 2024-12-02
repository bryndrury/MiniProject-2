import time
import json
import zlib
import uproot
import vector
import awkward as ak

import infofile
from HZZ.definitions import samples, variables, weight_variables, MeV, GeV, lumi, path

# ============================================================================== #
# For Worker:

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

def calc_weight(weight_variables, sample, events, lumi=10):
    info = infofile.infos[sample]
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    total_weight = xsec_weight 
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

# def work_on_data(val, data, sample_data, start) -> None:
#     cutoffs = [30, 20, 10]
    
#     # Number of events in this batch
#     nIn = len(data) 
                            
#     # Record transverse momenta (see bonus activity for explanation)
#     data['leading_lep_pt'] = data['lep_pt'][:,0]
#     data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
#     data['third_leading_lep_pt'] = data['lep_pt'][:,2]
#     data['last_lep_pt'] = data['lep_pt'][:,3]
    
#     data = data[data['leading_lep_pt'] * MeV > cutoffs[0]]
#     data = data[data['sub_leading_lep_pt'] * MeV > cutoffs[1]]
#     data = data[data['third_leading_lep_pt'] * MeV > cutoffs[2]]

#     # Cuts
#     lep_type = data['lep_type']
#     data = data[~cut_lep_type(lep_type)]
#     lep_charge = data['lep_charge']
#     data = data[~cut_lep_charge(lep_charge)]
    
#     # Invariant Mass
#     data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

#     # Store Monte Carlo weights in the data
#     if 'data' not in val: # Only calculates weights if the data is MC
#         data['totalWeight'] = calc_weight(weight_variables, val, data)
#         nOut = sum(data['totalWeight']) # sum of weights passing cuts in this batch 
#     else:
#         nOut = len(data)
#     elapsed = time.time() - start # time taken to process
#     print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)+"\t in "+str(round(elapsed,1))+"s") # events before and after
    

#     # Append data to the whole sample data list
#     sample_data.append(data)

def work_on_file(s, val, fraction, step_size) -> ak.Array:
    cutoffs = [30, 20, 10]
    
    if s == 'data': 
        prefix = "Data/" # Data prefix
    else: # MC prefix
        prefix = "MC/mc_"+str(infofile.infos[val]["DSID"])+"."
    fileString = path+prefix+val+".4lep.root" # file name to open


    # start the clock
    start = time.time() 
    print("\t"+val+":") 

    # Open file
    # with uproot.open(fileString + ":mini") as t:
    #     tree = t
    t = uproot.open(fileString + ":mini")
    tree = t
    
    sample_data = []

    for data in tree.iterate(variables + weight_variables, 
                            library="ak", 
                            entry_stop=tree.num_entries*fraction, # process up to numevents*fraction
                            step_size = step_size): 
        # work_on_data(val, data, sample_data, start)
        # Number of events in this batch
        nIn = len(data) 
                                
        # Transverse momentum records and cuts
        data['leading_lep_pt'] = data['lep_pt'][:,0]
        data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
        data['third_leading_lep_pt'] = data['lep_pt'][:,2]
        data['last_lep_pt'] = data['lep_pt'][:,3]

        data = data[data['leading_lep_pt'] * MeV > cutoffs[0]]
        data = data[data['sub_leading_lep_pt'] * MeV > cutoffs[1]]
        data = data[data['third_leading_lep_pt'] * MeV > cutoffs[2]]

        # Cuts
        lep_type = data['lep_type']
        data = data[~cut_lep_type(lep_type)]
        lep_charge = data['lep_charge']
        data = data[~cut_lep_charge(lep_charge)]
        
        # Invariant Mass
        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

        # Store Monte Carlo weights in the data
        if 'data' not in val: # Only calculates weights if the data is MC
            data['totalWeight'] = calc_weight(weight_variables, val, data, lumi)
            nOut = sum(data['totalWeight']) # sum of weights passing cuts in this batch 
        else:
            nOut = len(data)
        elapsed = time.time() - start # time taken to process
        print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)+"\t in "+str(round(elapsed,1))+"s") # events before and after

        # Append data to the whole sample data list
        sample_data.append(data)

    tree.close() # Ensure the file it closed
    return ak.concatenate(sample_data)

def process_incomming_request(body):
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
    
    return message_compressed, s, val