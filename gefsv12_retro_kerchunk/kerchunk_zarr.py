import asyncio
import base64
import datetime
import glob
import json
import os
import pkgutil
import re
from tempfile import TemporaryDirectory
from typing import List, Optional, Union

import fsspec
import nest_asyncio
import numpy as np
import pandas as pd
import pytz
import ujson
from kerchunk.combine import MultiZarrToZarr

from . import utils

base_s3_reforecast = "s3://noaa-gefs-retrospective/GEFSv12/reforecast/"
fs_read = fsspec.filesystem(
    "s3", anon=True, skip_instance_cache=True, asynchronous=True
)
fs_local = fsspec.filesystem("", skip_instance_cache=True, use_listings_cache=False)
nest_asyncio.apply()


class RetrospectivePull:
    """
    Generates metadata and pulls the GEFS Retrospective from AWS Open Data for a specific date and time range
    in extremely fast fashion due to consistency in the data structure (and no new updating files).
    Attributes:
        date (Union[datetime.datetime, pd.DatetimeIndex]): The date for which to pull the data. Defaults to the current date and time in UTC.
        fhour (int): Forecast hour. Defaults to 0.
        directory (Optional[str]): Directory to store temporary files. If None, a temporary directory is created.
        variable (str): The variable to pull. Defaults to "pres_msl".
        mode (str): Mode of operation. Defaults to "mclimate_default".
        centered_date_range (int): Range of dates centered around the given date. Defaults to 10.
        forecast_horizon (str): Forecast horizon. Defaults to "Days:1-10".
        members (Union[None, str, List[str]]): List of members to pull. Defaults to ["c00", "p01", "p02", "p03", "p04"].
    Methods:
        date_to_glob_pattern(date: datetime.datetime) -> list:
            Ingests a single date and returns a list of month-day combinations.
        generate_reforecast_uris(glob_patterns):
            Generates URIs for reforecast data based on glob patterns.
        async work_coroutine():
            Asynchronous coroutine to fetch data and file info concurrently.
        open_rep_file():
            Opens and reads the representative JSON file.
        generate_file(file, file_name):
            Generates a JSON file from the given data and saves it to the specified file name.
        generate_json_files():
            Generates JSON files for the reforecast data.
        generate_kerchunk(ds: bool = False):
            Generates kerchunk metadata and optionally returns an xarray dataset.
    """

    def __init__(
        self,
        date: Union[datetime.datetime, pd.DatetimeIndex] = datetime.datetime.now(
            tz=pytz.UTC
        ),
        fhour: int = 0,
        directory: Optional[str] = None,
        variable: str = "pres_msl",  # currently only for mslp but will change in the future
        mode: str = "mclimate_default",
        centered_date_range: int = 10,
        forecast_horizon: str = "Days:1-10",
        members: Union[None, str, List[str]] = None,
    ):
        if directory is None:
            self.td = TemporaryDirectory()
            self.directory = self.td.name
        self.date = date
        self.fhour = fhour
        self.message_num = self.fhour_to_message_num()
        self.variable = variable
        self.representative_json_name = f"assets/representative_{self.variable}.json"
        self.representative_json_data = self.open_rep_file()
        self.mode = mode
        self.centered_date_range = centered_date_range
        self.forecast_horizon = forecast_horizon
        if self.mode == "mclimate_default":
            self.glob_pattern = self.date_to_glob_pattern(self.date)
        if members is None:
            self.members = ["c00", "p01", "p02", "p03", "p04"]
        elif isinstance(members, str):
            self.members = list(members)
        else:
            self.members = members
        self.reforecast_urls = self.generate_reforecast_uris(self.glob_pattern)
        self.idx_files, self.files_info = asyncio.run(self.work_coroutine())
        self.file_sizes = [n["size"] for n in self.files_info]
        self.files_metadata_dict = dict(zip(self.idx_files, self.file_sizes))

    def fhour_to_message_num(self) -> int:
        """Converts forecast hour to message number"""
        assert self.fhour != 0, "No hour 0 forecast available"
        assert self.fhour % 3 == 0, "Forecast hour must be divisible by 3"
        message_num = ((self.fhour) // 3) - 1
        return message_num

    def date_to_glob_pattern(self, date: datetime.datetime) -> list:
        """Ingests a single date and returns a list of month-day combinations"""
        datetime_min = date - datetime.timedelta(days=self.centered_date_range)
        datetime_max = date + datetime.timedelta(days=self.centered_date_range)
        datetime_range = pd.date_range(datetime_min, datetime_max, freq="D")
        month_day_combinations = set(
            zip(datetime_range.month, datetime_range.day)  # pylint: disable=no-member
        )
        glob_patterns = [f"{month:02}{day:02}" for month, day in month_day_combinations]
        return glob_patterns

    def generate_reforecast_uris(self, glob_patterns):
        """
        Generate a list of URIs for reforecast data based on provided glob patterns.
        This method constructs URIs for reforecast data stored in an S3 bucket. It
        iterates over a range of years, applies glob patterns, and incorporates
        forecast members and forecast horizons to generate the final URIs.
        Args:
            glob_patterns (list of str): List of glob patterns to match specific
                         reforecast data files.
        Returns:
            list of str: A list of URIs pointing to the reforecast data files.
        """

        reforecast_patterns = [
            f"{base_s3_reforecast}{year}/{year}" for year in range(2000, 2020)
        ]
        reforecast_patterns_w_days = [
            f"{yearly_rf_patterns}{globs}00/"
            for yearly_rf_patterns in reforecast_patterns
            for globs in glob_patterns
        ]
        reforecast_patterns_w_members_horizon = [
            f"{daily_rf_patterns}{member}/{self.forecast_horizon}/"
            for daily_rf_patterns in reforecast_patterns_w_days
            for member in self.members
        ]
        reforecast_patterns_w_members_horizon_variable = [
            f"{subpath}{self.variable}_{subpath.split('/')[6]}_{subpath.split('/')[7]}.grib2.idx"
            for subpath in reforecast_patterns_w_members_horizon
        ]
        return reforecast_patterns_w_members_horizon_variable

    async def work_coroutine(self):
        session = await fs_read.set_session()  # Creates the client

        # Define tasks for fetching data and getting file info concurrently
        data_task = fs_read._cat(self.reforecast_urls)  # Fetches data concurrently
        info_tasks = [
            fs_read._info(path) for path in [rf[:-4] for rf in self.reforecast_urls]
        ]  # Gets info concurrently
        file_info = await asyncio.gather(*info_tasks)  # Runs all info tasks in parallel

        out = await data_task  # Await the data task
        await session.close()  # Explicit destructor for cleanup

        return out, file_info

    def open_rep_file(self):
        data_bytes = pkgutil.get_data(__name__, self.representative_json_name)
        data_str = data_bytes.decode("utf-8")
        representative_json_data = json.loads(data_str)
        return representative_json_data

    def generate_file(self, file, file_name):
        outf = os.path.join(self.directory, file_name)
        with fs_local.open(outf, "w") as f:
            f.write(ujson.dumps(file, reject_bytes=False))

    def generate_json_files(self):
        file_locations = list(self.idx_files.keys())
        data_to_replace = self.open_rep_file()
        for file_location in file_locations:
            message_nums = [
                n.split(":")[1]
                for n in self.idx_files[file_location].decode("utf-8").split("\n")[:-1]
            ]
            message_length = self.files_metadata_dict[file_location]
            date_string = file_location.split("_")[2]
            formatted_date = f"{date_string[:4]}-{date_string[4:6]}-{date_string[6:8]}T{date_string[8:]}"
            npdt64date = np.datetime64(formatted_date, "s")
            for i, n in enumerate(message_nums):
                if i == self.message_num:
                    step_str = (
                        self.idx_files[file_location]
                        .decode("utf-8")
                        .split("\n")[i]
                        .split(":")[5]
                    )
                    step = int("".join(x for x in step_str if x.isdigit()))
                    nptd64step = npdt64date + np.timedelta64(step, "h")
                    if i == 0:
                        message_range = ["{{u}}", 0, int(message_nums[1])]
                    elif i == len(message_nums) - 1:
                        message_range = [
                            "{{u}}",
                            int(message_nums[i]),
                            int(message_length - int(message_nums[i])),
                        ]
                    else:
                        message_range = [
                            "{{u}}",
                            int(message_nums[i]),
                            int(int(message_nums[i + 1]) - int(message_nums[i])),
                        ]
                    data_to_replace["refs"]["msl/0.0"] = message_range
                    data_to_replace["templates"] = {"u": f"s3://{file_location[:-4]}"}
                    data_to_replace["refs"]["time/0"] = b"base64:" + base64.b64encode(
                        npdt64date
                    )
                    data_to_replace["refs"]["valid_time/0"] = (
                        b"base64:" + base64.b64encode(nptd64step)
                    )
                    data_to_replace["refs"]["step/0"] = b"base64:" + base64.b64encode(
                        np.timedelta64(step, "h")
                    )
                    self.generate_file(
                        data_to_replace,
                        f"{file_location.split('/')[7].split('.')[0]}_{i:02}.json",
                    )

    def generate_kerchunk(self, ds: bool = False):
        pattern = re.compile(r"[A-Za-z]\d\d(?![^ ]*[\\\/])", re.IGNORECASE)
        file_list = glob.glob(f"{self.directory}/*")
        mzz = MultiZarrToZarr(
            file_list,
            coo_map={"member": pattern},
            concat_dims=["member", "step", "time"],
            identical_dims=["latitude", "longitude"],
        )
        multi_kerchunk = mzz.translate()
        if ds:
            ds = utils.create_xarray_from_kerchunks(multi_kerchunk)
            return ds
        else:
            return multi_kerchunk


if __name__ == "__main__":
    retro_pull = RetrospectivePull()
