# docker_kafka_assesment
## Pipeline Diagram
![diagram](https://github.com/cory-hall/docker_kafka_assesment/blob/main/resources/Kafka_Docker_Assesment.png?raw=true)

## Required Tools:
- An IDE that can run Python, I used [Visual Studio Code](https://code.visualstudio.com/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Python](https://www.python.org/downloads/)

## Dependency Installation:
Once you have an IDE setup and Python installed you will need to install a few dependencies required for this to run. To do this, run a terminal in `bash` and run the following commands.

- `pip install jsonschema`
- `pip install kafka-python-ng`
- `pip install pandas`

## Next Steps:
After you have these dependencies installed you should no longer see any yellow marks showing up in any file. <br>
The next steps involve running each file independently so you can see the logging that is directed to the terminal within your IDE. <br>
You will need 5 separate instances of terminal running `bash` all at the `root` level of the project (i.e. your IDE should show `~/docker_kafka_assesment` within the terminal. <br>

## Running Python Files
Run each command in a separate terminal window.

- `docker-compose up -d` This starts the docker service as well as the initial Kafka server to consume the data.
- `python consumer/kafka_consumer.py` This will begin to consume and cleanse the data, and stream it futher down our project.
- `python analyze_data_script.py` This will display a few of the insights that I was able to discover within the data.
- `python process_data_script.py` This will begin a few data transformations as well as send the transformed data further down. May require to run the script twice.
- `python producer/kafka_producer.py` This is the final step and consumes the transformed data and sends it to a new Kafka topic.

## Design Choices
I went with utilizing Kakfa to stream data between all functionality of the pipeline, as I feel like this has the best potential for scalability as well as ease of use within Docker containers. I tried my best to keep all of the code separate and modular to help reduce code debt in the future as well as support high flexability. I have many implementations for error handling and fault tolerance. I utilized DLQs (Dead Letter Queries) that crate a dedicated topic so we dont lose data as well as having the ability to go back and study the failed message in order to diagnose issues. I implemented retry mechanisms that exponentially get longer between each but aren't infinite so the bad messages can eventually reach the DLQ. I also implemented Schema validation so we can guarantee that the data coming in is within the boundaries of data governence. Lastly I was very liberal with the logging for testing and validation purposes. Within a production environment I would develop a more elegant solution instead of displaying every single message processed.

## Additional Questions
1. I would first get clarity on the available resources for this product as we may need to adjust configurations depending on message consumption vs allotted resources. Then comes adjusting the logging - Logging is important but you need it to be clear and conscise. You don't want to see every message transaction, but you do need to clearly see errors and have them stragecially placed to assist with debugging and troubleshooting. You also need to consider security. Does any of this data need to be encrypted or masked? If so, implement those changes. I would also utilize some sort of Service Manager like Kubernetes or if on the cloud, their respective orchestration service.
2. I mentioned a few ideas up top, but something like an orchestrator to automate deployment as well as looking into security concerns.
3. I believe witht the proper resources, yes this pipeline can scale effeciently with growing data sets.

