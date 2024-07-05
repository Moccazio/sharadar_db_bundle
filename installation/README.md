# Installation instructions

Using [mamba](https://github.com/mamba-org/mamba) in [conda environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) based on the [Miniconda](https://docs.conda.io/en/latest/miniconda.html) distribution and the provided `py310.yml` environment files,

   run:
   - ```bash
     conda create -n py310 python=3.10
     mamba env update -n py310 -f py310.yml
     conda activate py310
     ```


### Install miniconda

The notebooks rely on a single virtual environment based on [miniconda3](https://docs.conda.io/en/latest/miniconda.html) that you need to install first. 

You can find detailed instructions for various operating systems [here](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).