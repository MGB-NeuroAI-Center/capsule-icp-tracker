import os
import ipdb; ipdb.set_trace
import dotenv
import polars as pl
import json
import matplotlib.pyplot as plt
import numpy as np
from datetime import date, timedelta

if __name__ == "__main__":

    dotenv.load_dotenv(f"{os.getcwd()}/config/.env")
    ICP_AGGS_FILE = os.getenv("ICP_AGGS_FILE")
    CSN_TARGET_1 = os.getenv("CSN_TARGET_1")
    CSN_TARGET_2 = os.getenv("CSN_TARGET_2")

    # =*= group by csns =*=
    icp_df = pl.read_parquet(f"{os.getcwd()}/data/{ICP_AGGS_FILE}")

    # =*= cast csns to str from int/list =*=
    CSNS = json.loads(os.getenv("CSNS"))
    icp_df = icp_df.with_columns(pl.col("csn").list.join(",").alias("csn")) # assume one csn per entry
    
    icp_df_csn = (icp_df.filter(pl.col("csn").is_in(CSNS)))
    icp_df_csn.filter(pl.col("csn") == CSN_TARGET_1).sort(by="start time")
    icp_df_csn_agg = (icp_df
        .filter(pl.col("csn").is_in(CSNS))
        .group_by(by="csn")
            .agg(
                pl.col("file").alias("file"),
                pl.col("icp time").sum().alias("total icp time"),
                pl.len().alias("rows"),
                pl.col('point of care').list.join(",").alias("point of care")))
    
    icp_df_csn.filter(pl.col("csn") == CSN_TARGET_1)

    # =*= scan parquet linked to one csn =*=
    for enc in range(icp_df_csn_agg.height):
        
        # =*= get filepaths =*=
        csn_fps = icp_df_csn_agg["file"][enc].to_list()

        for fp in csn_fps:

            fp = icp_df_csn.filter(pl.col("csn") == CSN_TARGET_2).sort(by="start time")["file"][0]
            fp = icp_df_csn.filter(pl.col("csn") == CSN_TARGET_2)["file"][-2]

            dci_df = pl.read_parquet(fp)

            icp_wv_df = dci_df.filter((pl.col("WaveID") == "51920") & (pl.col("ChannelID") == "2"))
            # table: medians, min-max, channel id 2 and 51920 waveid

            # =*= cast data(str) to reported type =*=
            # =*= max precision =*=
            dtype_map = {
                "int": pl.Int64, # pl.Int64
                "float": pl.Float64}

            df_dtypes = icp_wv_df["DataType"].unique().to_list()
            dtype_ord = [pl.Int32, pl.Int64, pl.Float32, pl.Float64]

            def get_dtype(dtype):
                if dtype in dtype_map:
                    return dtype_map[dtype]
                return getattr(pl, dtype.capitalize())

            # =*= retain og data type, else Float64 (mem issue) =*=
            icp_dataconv_df = icp_wv_df.with_columns(
                pl.col("Data")
                    .str.split(",")
                    .list.eval(
                        pl.element()
                        .str.strip_chars()
                        .cast(pl.Float64 if len(df_dtypes) > 1 # mixed-dtype handling
                            else get_dtype(df_dtypes[0]))) # hash out list[0]
                .alias("Data"))

            # =*= "passed" channel should be 2 by icp-def crit =*=

            # =*= sort by NeuronTime (or assume sorted) =*=
            icp_sort_df = icp_dataconv_df.sort(by="NeuronTime")

            # =*= concat data =*=
            icp_data = icp_sort_df.select(pl.col("Data").list.explode()).to_series()

            """
            icp_ts_df = (icp_sort_df
                .with_columns(
                    (pl.col("MeasurementTime") - pl.col("NeuronTime")).alias("MessageSpan"),
                    (pl.col("Data").len().alias("MessageLength"))
                    )
                .with_columns(
                    (pl.col("MessageSpan")/ pl.col("MessageLength").sub(1)).alias("MessageStep"))
                .with_columns(
                    pl.struct([
                        pl.col("MeasurementTime"),
                        pl.col("NeuronTime"),
                        pl.col("MessageStep")])
                    .map_elements(lambda row:
                        pl.datetime_range(
                            row["NeuronTime"],
                            row["MeasurementTime"],
                            row["MessageStep"],
                            time_unit = 'ns'))
                    .alias("MessageTimes")))
            """

            plt.plot(icp_data)
            plt.show()