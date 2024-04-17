# What is terraform

[terraform.io](https://www.terraform.io/) allows to define the infrastructure of your project using code.

In other words, it allows you to avoid the gui-click nightmare you have to through if you want to setup an infrastructure on GCS, AWS, Azure or similar.

Instead, you (1) install Terraform locally, (2) download the code to interact with the cloud provider you want to work with , (3) provide Terraform your keys (like a .json) to interact with the cloud provider.

At this point, you write terraform tf files with specifications of the desired infrastructure, and they will instruct your cloud provider to create that infrastructure.

This also means that Terraform uses a  **declarative style** : you tell it *what* you want, and it will take care of *how* to get it done.

Check out other advantages of tf in [this video](https://www.youtube.com/watch?v=nvNqfgojocs)

We can download the binary for terraform [here](https://developer.hashicorp.com/terraform/install) and then move it into a path dir, e.g. `/usr/local/bin`

## Terraform Primer - Concepts and Overview

[video](https://www.youtube.com/watch?v=s2bOYDCKl_M)

Key Terraform commands:

* Init : get the code to access the specific provider once you have indicated where the keys (a .json file) are on your machine
* Plan : shows the resources that will be created
* Apply : create the infrastructure defined in the tf files on the cloud provider platform
* Destroy : remove everything which is defined in the tf files
