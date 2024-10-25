import xarray as xr
from dask.distributed import Client, LocalCluster


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
