# Big Data Analysis using Cloud Technology 
## SCIFM0004  - Distributed Computing Mini-project 2

Based on the code found in this [Notebook](https://github.com/atlas-outreach-data-tools/notebooks-collection-opendata/blob/master/13-TeV-examples/uproot_python/HZZAnalysis.ipynb) provided by ATLAS Open Data programme, from the ATALS Collaboration.

### Usage:
#### To build and deploy:

	docker-compose up --build

#### To just build:

For all Images:

	docker-compose build

For individual: (i.e. service = worker)

    docker-compose build <service>

#### Running:

Completely Automated (Recommended): 
(This will automatically create the manager, and shutdown the workers once the manager has received all the results.)

    docker-compose up

Manual: 
(Runs the workers and services in the background waiting for work. The second command runs and attaches to the manager, in theory you could run multiple managers. This method leaves the workers running for other computations.) 

	docker-compose up --scale worker=6 --scale manager=0 -d
	docker-compose run --rm manager