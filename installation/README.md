**Installation Instructions**

These steps will guide you through setting up the environment using [Miniconda](https://docs.conda.io/en/latest/miniconda.html).

## **1\. Install Miniconda**

The notebooks rely on a single virtual environment based on **Miniconda3**. You must install this first.

* **General Instructions:** For Windows, Intel Macs, or Linux, find detailed instructions [here](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).  
* **macOS (Apple Silicon / M1 or later):** Open your Terminal and run the following commands:  
  Bash  
  mkdir \-p \~/miniconda3  
  curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh \-o \~/miniconda3/miniconda.sh  
  bash \~/miniconda3/miniconda.sh \-b \-u \-p \~/miniconda3  
  rm \-rf \~/miniconda3/miniconda.sh

### **Initialize Shell**

After installing, initialize Miniconda for your specific shell (bash or zsh):

Bash

\~/miniconda3/bin/conda init bash  
\~/miniconda3/bin/conda init zsh

**Note:** You may need to restart your terminal for these changes to take effect.

## ---

**2\. Create and Configure the Environment**

Using the provided py310.yml environment file, run the following commands to create the environment, install dependencies, and activate it.

Bash

\# 1\. Create the base Python 3.10 environment  
conda create \-n py310 python=3.10

\# 2\. Update the environment using the yaml file  
\# (Note: This might take a few minutes as conda solves the environment)  
conda env update \-n py310 \-f py310.yml

\# 3\. Activate the environment  
conda activate py310

