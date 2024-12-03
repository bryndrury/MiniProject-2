To build and deploy:
	docker-compose up --build

To just build:
-> For all Images:
	docker-compose build

-> For individual: -> (i.e. service = worker)
	docker-compose build <service>

Running:
-> Completely Autonmated (Recomended): -> (This will automatically create the manager, and shutdown the workers 
							  once the manager has recieved all the reasult.)
	docker-compose up

-> Manual: -> (Runs the workers and services in the background waiting for work.
				   The second command runs and attaches to the manager, in theory you could run multiple managers.
				   This method leaves the workers running for other computations.) 
	docker-compose up --scale worker=6 --scale manager=0 -d
	docker-compose run --rm manager

-> To make manual pernanent:
#### Change the replicas value for manager from 1 to 0 in the docker-compose.yml, ####
#### then comment-out line 65 in manager_processing.py in the HZZ directory. ####
	docker-compose up --build -d
	docker-compose run --rm manager

Message compression is not active by default. To change, alter the variable use_compression in the manager.py script.
