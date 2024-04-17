The aim here is to create the Google Cloud environment we will subsequently use. It can be summarised in a few steps:

* Create a new project and copy the .ssh pub key
* Create a VM
* Install anaconda (or miniconda), docker, terrraform
* remotely connect via the Remote SSH extension in VS Code, and open the ports we will need (e.g. 8080 for pgAdmin and 8888 for Jupyter Notebook)
* run a first terraform tf file for infrastructure creation
* learn how to stop or destroy the VM

> When you create a VM and stop it, you will not pay for the computing time, but you will still pay for the storage space (although the cost is much smaller).

[Intro video on the Google Cloud Platform](https://www.youtube.com/watch?v=18jIzE41fJ4) (~ 3 min)

Access the google cloud console here : [console.cloud.google.com](http://console.cloud.google.com/)

- email - yash95kumar6@gmail.com
- user  - yash
- ssh key (de_zoomcamp_gcp.pub)  - stored in .ssh folder on computer
- Add Public SSH key to the GCP



## Creating a new project and a VM

First of all create a new project. I called it `dataengineering-zoomcamp-2024`

Copy an ssh public key to Google Cloud. To do so, add an ssh_pub key to the project from the **Metadata** menu in the left panel. I created a `DE_zoomcamp.pub` key just for this zoomcamp.

[![](https://github.com/leonardocerliani/de_zoomcamp_2024/raw/main/01b_GoogleCloud_Setting/imgs/01.png)](https://github.com/leonardocerliani/de_zoomcamp_2024/blob/main/01b_GoogleCloud_Setting/imgs/01.png)

In the **VM instances** create an instance

[![](https://github.com/leonardocerliani/de_zoomcamp_2024/raw/main/01b_GoogleCloud_Setting/imgs/02.png)](https://github.com/leonardocerliani/de_zoomcamp_2024/blob/main/01b_GoogleCloud_Setting/imgs/02.png)

Select **europe-west4 (Netherlands) region** - US$ 27.37 a month (one of the cheapest) and select a small machine.

[![](https://github.com/leonardocerliani/de_zoomcamp_2024/raw/main/01b_GoogleCloud_Setting/imgs/03.png)](https://github.com/leonardocerliani/de_zoomcamp_2024/blob/main/01b_GoogleCloud_Setting/imgs/03.png)

Switch Boot disk to Ubuntu 20.04 LTS and make sure you set the disk to 30 GB (very little cost difference)

[![](https://github.com/leonardocerliani/de_zoomcamp_2024/raw/main/01b_GoogleCloud_Setting/imgs/04.png)](https://github.com/leonardocerliani/de_zoomcamp_2024/blob/main/01b_GoogleCloud_Setting/imgs/04.png)

The generated external IP is 35.230.35.28

 **NB: the IP will change every time you destroy and create a new VM** . This means that if you use a `.ssh/config` file - as we see below, e.g. to connect via VS code - you need to update this file accordingly

In order to connect via ssh, you need to provide the identity - name of the key you generated - as well as the name defined in the key:

```shell
ssh -i ~/.ssh/de_zoomcamp_gcp yash@35.230.35.28
```

To access in a simpler way, create a  **config file in .ssh** . This will allow us to connect to the VM using `ssh de-zoomcamp`

```shell
# .ssh/config
Host de-zoomcamp
	HostName 34.91.191.113
	User machinery
	IdentityFile ~/.ssh/DE_zoomcamp 
```

Install **Anaconda** by downloading and executing the bash installer (actually I installed **[Miniconda](https://docs.conda.io/projects/miniconda/en/latest/)** and then installed jupyter in the base environment)

Install **Docker**

```shell
sudo apt update
sudo apt install docker.io
```

Clone the course repo

```shell
git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git
```

Add the current user to the docker group, so that we don’t need to sudo docker every time

```shell
sudo gpasswd -a $USER docker
sudo service docker restart
# then logout and login again to activate the membership
```

You can then test that docker is working e.g. with

```shell
docker run --rm -it alpine:latest sh
```

Install [docker compose](https://github.com/docker/compose/releases)

`wget https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 -O docker-compose`

Remember to such chmod +x and move it to a location in the PATH, e.g. `/usr/bin`

The you can docker-compose up the content of the docker directory inside the cloned git repo

Install pgcli

`pip install pgcli`

Another method is proposed with conda, but conda takes forever on my mini-machine, and the pip does not complain, so I will go with that.

To test pgcli

```shell
pgcli -h localhost -U root -d ny_taxi
# pw of root is root
root@localhost:ny_taxi> \dt
root@localhost:ny_taxi> \q
```

FWD ports to the google cloud machine (this is magic!)

If we open the GCL machine *into* VS code and open a terminal, we will see that we can also fw local ports, so that we can e.g. access the services running on the GCL machine in localhost.

[![](https://github.com/leonardocerliani/de_zoomcamp_2024/raw/main/01b_GoogleCloud_Setting/imgs/05.png)](https://github.com/leonardocerliani/de_zoomcamp_2024/blob/main/01b_GoogleCloud_Setting/imgs/05.png)

Then we can e.g. go to [localhost:8080](http://localhost:8080/) in our local browser and access pgadmin!!!

([admin@admin.com](mailto:admin@admin.com) / root)
