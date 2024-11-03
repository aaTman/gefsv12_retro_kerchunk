import xarray as xr
from dask.distributed import Client, LocalCluster
import fsspec
import ujson
from kerchunk.grib2 import scan_grib
from typing import List, Optional, Union

def create_xarray_from_kerchunks(fo):
    """creates a dataset based on the input location for the kerchunk zarr approach"""
    backend_args = {
        "consolidated": False,
        "storage_options": {
            "fo": fo,
            "remote_protocol": "s3",
            "remote_options": {"anon": True},
        },
    }
    ds = xr.open_dataset(
        "reference://", engine="zarr", backend_kwargs=backend_args, chunks="auto"
    )
    ds = ds.unify_chunks()
    return ds


def start_dask_cluster(
    n_workers: int = 8, threads_per_worker: int = 2, memory_limit: str = "2GiB"
):
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
    )
    client = Client(cluster)
    return client

def create_representative_json(
        file_url: str, output_file: str, grib_filter: dict =  {"typeOfLevel": "meanSea"}, message_number: int = 0
):
    """creates a representative json file for the kerchunk zarr approach"""
    base_s3_reforecast = "s3://noaa-gefs-retrospective/GEFSv12/reforecast/"
    fs_read = fsspec.filesystem(
        "s3", anon=True, skip_instance_cache=True, asynchronous=True
    )
    fs_local = fsspec.filesystem("", skip_instance_cache=True, use_listings_cache=False)
    so = {"anon": True}
    out = scan_grib(file_url, storage_options=so, filter=grib_filter)
    for i, message in enumerate(out):
        if i == message_number:
            with fs_local.open(output_file, "w") as f:
                f.write(ujson.dumps(message))