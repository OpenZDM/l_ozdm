# Building the package

`$ conda build .`

To install, use custom channels: [https://stackoverflow.com/questions/35359147/how-can-i-host-my-own-private-conda-repository](https://stackoverflow.com/questions/35359147/how-can-i-host-my-own-private-conda-repository).

# Example:

```
$ cp /Users/rlopes/miniconda3/envs/poselib/conda-bld/osx-arm64/poselib-0.0.2-py310_1.tar.bz2 /Users/rlopes/miniconda3/rp_channel/osx-arm64 
$ conda index /Users/rlopes/miniconda3/rp_channel
```

The channel directory should be mentioned in .condarc

```
channels:
- file://Users/rlopes/miniconda3/rp_channel
- defaults

```

# Requirements

To be able to build and package as a conda library, it is necessary to have the proper versions of `avro` and `stomp.py`.

For that, a conda package can be built based on the *pipy* repository:
```shell
$ conda skeleton pypi stomp.py
$ conda build stomp.py
$ cp /Users/rlopes/miniconda3/envs/ozdmlib/conda-bld/osx-arm64/stomp.py-8.1.0-py310_0.tar.bz2 /Users/rlopes/miniconda3/rp_channel/osx-arm64
$ conda index /Users/rlopes/miniconda3/rp_channel 
```