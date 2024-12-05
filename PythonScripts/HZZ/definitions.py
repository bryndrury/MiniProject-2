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