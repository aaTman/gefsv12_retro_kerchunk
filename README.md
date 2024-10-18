# GEFSv12 Retrospective, Kerchunkified

This module will allow you to rapidly pull an xarray dataset of GEFSv12 Retrospective data into your python file/notebook/etc. The secret is asynchronous pulls of the .idx files, having a representative kerchunk-sourced json of the grib file to rapidly modify, and building out n jsons using that template.

Right now, it can only do MSLP. This will expand in the future.

Simply:

```python
from gefsv12_retro_kerchunk.kerchunk_zarr import RetrospectivePull
retro = RetrospectivePull()
retro.generate_json_files()
ds = retro.generate_kerchunk(return_ds=True)
```

This pulls a 21 day window centered on the current date for MSLP: all 20 years of data, all latitudes and longitudes, all 5 members, for one time step (default is hour 0). In this approach, members are currently set to represent the exact string in the file name (e.g. c00, p01, etc.). For the default case, which is 420 unique times, 5 members, for the entire globe, this took 34.2 seconds to run on an i9-10900k (10 cores, 20 threads), 32gb RAM, GTX 1080ti personal computer.

Note: running something like ds.mean(dim='member') without instantiating a dask LocalCluster will probably blow your computer up if running locally. To address this fun quirk:

```python
import gefsv12_retro_kerchunk.utils as ut
client = ut.start_dask_clusters(
    n_workers=8, threads_per_worker=2, memory_limit="2GiB"
)
```

Will get that spun up for you as a convenient wrapper. You can also instantiate a LocalCluster on your own for more control over your environment.

## Installation

```
git clone https://github.com/aaTman/gefsv12_retro_kerchunk.git
cd gefsv12_retro_kerchunk
pip install -e .
```

PyPi soon! I want to set up testing and make sure more cases are handled beyond MSLP.
