# 1. Intro to Docker

Docker is a powerful containerization application, which provides a lightweight and efficient alternative to traditional virtual machines (VMs) like VMware. Containerization is a form of virtualization that allows applications to be packaged with their dependencies, libraries, and configurations into isolated, self-contained environments.

The key difference between Docker and VMs is that Docker containers do not require the emulation of a complete operating system. Instead, they leverage the host operating system's kernel, sharing its resources and running as isolated processes. This approach makes Docker containers significantly more lightweight and resource-efficient compared to traditional VMs, which need to simulate an entire virtualized computer.

By encapsulating an application and its dependencies within a Docker container, developers can ensure consistent and reliable deployment across different environments, from development to production. This streamlined approach to application packaging and distribution can greatly improve the efficiency and scalability of software development and deployment processes.

![Docker Architecture](https://www.docker.com/wp-content/uploads/2021/11/docker-containerized-appliction-blue-border_2.png)

Docker Cheat sheet - [here](https://docs.docker.com/get-started/docker_cheatsheet.pdf) and chatgpt docs - [chat](https://chat.openai.com/share/1ce5087e-2ad2-4db7-a7a5-af5a3ddf64cc)

***Documentation***- [here](https://docs.docker.com/guides/get-started/)

## 1.1 Python Container from the command line

To run a python container, we can simply do it from the command line, providing the -it argument to make it interactive:

```
docker run --rm -it python:3.9

# --rm is used to remove the container after it is exited
# -it to run in interactive mode
```

If it's not present locally, docker will pull the container from dockerhub.

However let's say we want to use Pandas, which is not installed in this image. In this case we need to install it manually once we are inside the container.

```
docker run --rm -it python:3.9 bash

# and from inside the container:
pip install pandas
```

However when we exit the container, Pandas will not be present anymore.

## 1.2 Custom image with Dockerfile

To ensure the persistence of the installed packages - among other things - we can define a Dockerfile.

Docker will first pull the python image, and then install pandas

```shell
# Dockerfile

FROM python:3.9

RUN pip install pandas

ENTRYPOINT [ "bash" ]
```

We build the image and then we run it:

```shell
docker build -f Dockerfile_initial -t de_test:pandas .
docker run --rm -it -t de_test:pandas

#-f to mention file name
#-t tag the image with a name
```


## 1.3 Running a local python script inside the container

Of course right now the container is not very useful, since it's not really doing anything.

Let's suppose that we want our container to carry out a very specific action: 

- run a pipeline defined by a python script called `pipeline.py` that we will load in the current folder.

To this aim, we can specify in the Dockerfile that every time the (soon-to-be-built) corresponding image is run, it should first copy the file from the directory where we run the image.

Also, we will instruct docker to run the command for executing the pipeline, instead of entering bash. Therefore we can also remove the interactivity.

Finally, to make it a bit more interesting, we can also pass an argument to the python script.

```python
# pipeline.py

import sys
import pandas as pd

day = sys.argv[1]
print(f'job finished succesfully for day = {day}')
```

```shell
# Dockerfile_initial

FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]
```

```shell
docker build -t test:pandas .
docker run --rm -t test:pandas "04/01/2014"
```

Basically in real case, we will pass the dokcerfile and all the other files like pipeline.py to any system and it will run the pipeline

# 2. Ingesting NY Taxi data into Postgres
