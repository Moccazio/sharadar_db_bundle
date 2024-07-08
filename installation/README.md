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

If you run on macOS M1 or later open Terminal and run:
   - ```bash
     mkdir -p ~/miniconda3
     curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh -o ~/miniconda3/miniconda.sh
     bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
     rm -rf ~/miniconda3/miniconda.sh
     ```

After installing, initialize your newly-installed Miniconda. The following commands initialize for bash and zsh shells:
- ```bash
  ~/miniconda3/bin/conda init bash
  ~/miniconda3/bin/conda init zsh
  ```
  
### Install mamba

[conda] is the package manager provided by the [Anaconda](https://www.anaconda.com/) python distribution. Unfortunately, it is currently [not in very good shape](https://github.com/conda/conda/issues/9707). Instead, we'll use the more recent and much faster [mamba](https://github.com/mamba-org/mamba) package manager to install packages. You can install it using:
```python
conda install -n base -c conda-forge mamba
```

