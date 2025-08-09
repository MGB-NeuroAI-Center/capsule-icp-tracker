import os
import json
from datetime import timedelta
from pathlib import Path, PureWindowsPath
import multiprocessing as mp

import polars as pl
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import dotenv
import ipdb

# =*= format .pq filepaths by os, wavelabel, date =*=
def format_fp(
        wavelabel_filt="Unspecified Invasive Blood Pressure Waveform (P)",
        date_filt=pl.date(2025,5,1)):

    # =*= parse capsule w- files for icp =*=
    wavelabel_df = pl.read_csv(f"{os.getcwd()}/data/wavelabel_counts.csv")

    pressure_df = (wavelabel_df if wavelabel_filt is None 
        else wavelabel_df.filter(pl.col("label") == wavelabel_filt))

    # =*= filter for files after april/may 2025 =*=
    date_df = (
        pressure_df
        .with_columns(
            pl.col("file")
            .str.replace(".parquet", "")
            .str.split("-")
            .list.slice(-3, 3) # (y,m,d)
            .list.eval(pl.element().cast(pl.Int32, strict=False))
        .alias("dt_parts"))
        .with_columns((
            pl.col("dt_parts").list.contains(None))
            .alias("cast_fail"))
        .filter(~pl.col("cast_fail"))
        .with_columns(
            pl.date(
                pl.col("dt_parts").list.get(0),
                pl.col("dt_parts").list.get(1),
                pl.col("dt_parts").list.get(2))
            .alias("date"))
        .drop("dt_parts","cast_fail")) # dropped one

    date_df = date_df.filter(pl.col("date") > date_filt)

    # =*= format filepaths for os =*=
    if os.name == "posix": # for mac-os
        # no drive (eg C:)
        DRIVE_PREFIX = os.getenv("DRIVE_PREFIX")

        pressure_df = date_df.with_columns(pl.col("file")
            .str.replace(DRIVE_PREFIX,"/Volumes",literal=True,n=1)
            .str.replace_all("\\", "/",literal=True)
            .alias("file")) # overwrite

    # =*= load capsule .parquet files =*=
    p_filepaths = pressure_df["file"].to_list()

    return p_filepaths

# =*= get .pq attrs =*=
def get_attrs(
        wv_df,
        wavelabel_filt="Unspecified Invasive Blood Pressure Waveform (P)"):

    # =*= since data may contain multiple wavelabels, filter for P =*=
    p_df = wv_df.filter(pl.col("WaveLabel")==wavelabel_filt)

    # =*= append csn, point of care, room, bed, facility =*=
    csn = sorted(p_df['CSN'].unique().to_list())
    poc = sorted(p_df['PointOfCare'].unique().to_list())
    room = sorted(p_df['RoomBed'].unique().to_list())
    bed = sorted(p_df['Bed'].unique().to_list())
    facil = sorted(p_df['Facility'].unique().to_list())
    dev_a = sorted(p_df['DeviceAttributes'].unique().to_list())
    wv_id = sorted(p_df['WaveID'].unique().to_list())

    # =*= get entire time span =*=
    begtime = wv_df['NeuronTime'].min()
    endtime = wv_df['MeasurementTime'].max()

    return p_df, csn, poc, room, bed, facil, dev_a, wv_id, begtime, endtime

# =*= identify icp waveforms =*=
def qual_icp(p_df):

    # =*= cast data(str) to reported type =*=
    # =*= max precision =*=
    dtype_map = {
        "int": pl.Int64, # pl.Int64
        "float": pl.Float64}

    df_dtypes = p_df["DataType"].unique().to_list()
    dtype_ord = [pl.Int32, pl.Int64, pl.Float32, pl.Float64]

    def get_dtype(dtype):
        if dtype in dtype_map:
            return dtype_map[dtype]
        return getattr(pl, dtype.capitalize())

    # =*= retain og data type, else Float64 (mem issue) =*=
    """old
    p_dataconv_df = pl.concat([
        p_df.filter(pl.col("DataType") == dtype)
            .with_columns(pl.col("Data")
                .str.split(",")
                .list.eval(
                    pl.element()
                    .str.strip_chars()
                    .cast(get_dtype(dtype)))
                .alias("Data"))
        for dtype in p_df["DataType"].unique().to_list()])
    """

    p_dataconv_df = p_df.with_columns(
        pl.col("Data")
            .str.split(",")
            .list.eval(
                pl.element()
                .str.strip_chars()
                .cast(pl.Float64 if len(df_dtypes) > 1 # mixed-dtype handling
                    else get_dtype(df_dtypes[0]))) # hash out list[0]
        .alias("Data"))

    # =*= sort by NeuronTime (or assume sorted) =*=
    # q: if we sort, err timestamps get misplaced. do we leave as is or...?
    p_sort_df = p_dataconv_df.sort(by="NeuronTime")

    # =*= identify icp based on data crit =*=
    p_mean_df = p_sort_df.with_columns(pl.col("Data").list.mean().alias("Mean"))

    # =*= group and filter means by ChannelID =*=
    ch_df = (p_mean_df
        .group_by(by="ChannelID")
        .agg(
            pl.col("Mean").mean().alias("ChannelMeans"),
            pl.col("Mean").median().alias("ChannelMedians"),
            pl.col("Mean").min().alias("ChannelMins"),
            pl.col("Mean").max().alias("ChannelMaxs"),
            pl.len().alias("Rows")) # =*= get total rows of qual icp chs =*=
        .with_columns(
            pl.col("ChannelMeans").is_between(0,25,'both').alias("IsValid"))
        .sort("ChannelMeans"))
    # table: medians, min-max, channel id 2 and 51920 waveid

    p_chs = ch_df.get_column("by").cast(pl.Int32).to_list()
    p_means = ch_df.get_column("ChannelMeans").to_list()
    p_meds = ch_df.get_column("ChannelMedians").to_list()
    p_mins = ch_df.get_column("ChannelMins").to_list()
    p_maxs = ch_df.get_column("ChannelMaxs").to_list()
    p_rows = ch_df.get_column("Rows").to_list()

    icp_df = ch_df.filter(pl.col("IsValid"))
    if icp_df.is_empty():
        incl_excl = "mean_crit_unmet"
        icptime = timedelta(seconds=0, microseconds=0)
    else:
        incl_excl = "pass"
        
        # =*= grab icp time span =*=
        icp_chs = icp_df.get_column("by").to_list()
        icp_df_chs = p_mean_df.filter(pl.col("ChannelID").is_in(icp_chs))
        icp_df_chs = icp_df_chs.with_columns(
            (pl.col("MeasurementTime") - pl.col("NeuronTime"))
            .alias("TimeDiff"))
        icptime = icp_df_chs.select(pl.sum("TimeDiff"))
        icptime = icptime.get_column("TimeDiff").item()

    # =*= next steps: quantify amount of data (sz, total pts, time span) =*=
    # mark rec dur via timestamps

    # =*= skip file if not between 0-25 (presumably mmHg) =*=

    return p_df, incl_excl, p_chs, p_means, p_meds, p_mins, p_maxs, p_rows, icptime

def one_pq(f):

    try:
        # =*= one parquet =*=
        wv_df = pl.read_parquet(f) # bottlenecks at read from capsule drive
        WAVELABELS = json.loads(os.getenv("WAVELABELS"))
        if not WAVELABELS: # if empty
            WAVELABELS = wv_df["WaveLabel"].unique() # varies by file

        res = []
        for wv_lbl in sorted(WAVELABELS):

            # =*= get .csv attrs =*=

            # =*= assume attrs do not differ locally =*=
            p_df, curr_csn, curr_poc, curr_room, curr_bed, curr_facil, curr_dev_a, curr_wv_id, curr_begtime, curr_endtime  = get_attrs(wv_df, wavelabel_filt=wv_lbl)

            # =*= get icp =*=
            p_df, curr_incl_excl, curr_p_chs, curr_p_means, curr_p_meds, curr_p_mins, curr_p_maxs, curr_p_rows, curr_icptime = qual_icp(p_df)    

            res.append({"csn": curr_csn,
                "start time": curr_begtime,
                "end time": curr_endtime,
                "icp time": curr_icptime,
                "device attribute": curr_dev_a,
                "wavelabel": wv_lbl,
                "device id": curr_wv_id,
                "point of care": curr_poc,
                "facility": curr_facil,
                "room": curr_room,
                "bed": curr_bed,
                "channel ids": curr_p_chs,
                "channel means": curr_p_means,
                "channel medians": curr_p_meds,
                "channel mins": curr_p_mins,
                "channel maxs": curr_p_maxs,
                "channel rows": curr_p_rows,
                "include/exclude": curr_incl_excl,
                "file": f})

        return res, None

    except Exception as e:

        err_res = {"file": f, "err": str(e)}

        return None, err_res


# =*= full implementations: sequential and parallel =*=

def by_seq(p_filepaths, w):
    # =*= sequential, one-by-one =*=

    # =*= init icp.csv columns =*=
    csn, dev_a, wv_id, wv_lbls = [], [], [], []
    begtime, endtime, icptime = [], [], []
    capsule_fp, incl_excl = [], []
    poc, room, bed, facil = [], [], [], []
    p_chs, p_means, p_meds, p_mins, p_maxs, p_rows = [], [], [], [], [], []

    # =*= collect bad files =*=
    bad_fp, fp_err = [], []

    # =*= loop .pq filepaths =*=
    for f in tqdm(p_filepaths):

        try:
            # =*= one parquet =*=
            wv_df = pl.read_parquet(f) # bottlenecks at read from capsule drive

            # =*= load wavelabels to iter =*=    
            WAVELABELS = json.loads(os.getenv("WAVELABELS"))
            if not WAVELABELS: # if empty
                WAVELABELS = wv_df["WaveLabel"].unique() # varies by file

            for wv_lbl in sorted(WAVELABELS):

                # =*= get .csv attrs =*=

                # =*= assume attrs do not differ locally =*=
                p_df, curr_csn, curr_poc, curr_room, curr_bed, curr_facil, curr_dev_a, curr_wv_id, curr_begtime, curr_endtime = get_attrs(wv_df, wavelabel_filt=wv_lbl)

                # =*= identify icp =*=
                p_df, curr_incl_excl, curr_p_chs, curr_p_means, curr_p_meds, curr_p_mins, curr_p_maxs, curr_p_rows, curr_icptime = qual_icp(p_df)

                # =*= compute % icp time spent =*=
                icp_timepct = curr_icptime/(curr_endtime - curr_begtime)

                # =*= appends if no err =*=
                capsule_fp.append(f)
                csn.append(curr_csn)
                dev_a.append(curr_dev_a)
                wv_lbls.append(wv_lbl)
                wv_id.append(curr_wv_id)
                poc.append(curr_poc)
                room.append(curr_room)
                bed.append(curr_bed)
                facil.append(curr_facil)
                incl_excl.append(curr_incl_excl)
                p_chs.append(curr_p_chs)
                p_means.append(curr_p_means)
                p_meds.append(curr_p_meds)
                p_mins.append(curr_p_mins)
                p_maxs.append(curr_p_maxs)
                p_rows.append(curr_p_rows)
                begtime.append(curr_begtime)
                endtime.append(curr_endtime)
                icptime.append(curr_icptime)

        except Exception as e:

            bad_fp.append(f)
            fp_err.append(str(e))

    # =*= exception handling =*=

    # =*= post-loop =*=
    # =*= build and write csv =*=
    icp_df = pl.DataFrame({
        "csn": csn,
        "start time": begtime,
        "end time": endtime,
        "icp time": icptime,
        "device attribute": dev_a,
        "wavelabel": wv_lbls,
        "device id": wv_id,
        "point of care": poc,
        "facility": facil,
        "room": room,
        "bed": bed,
        "channel ids": p_chs,
        "channel means": p_means,
        "channel medians": p_meds,
        "channel mins": p_mins,
        "channel maxs": p_maxs,
        "channel rows": p_rows, # row entries per ch
        "include/exclude": incl_excl, # entries: mean_crit_unmet, pass
        "file": capsule_fp})

    bad_df = pl.DataFrame({
        "file": bad_fp,
        "error": fp_err})

    if w:
        
        ICP_SEQ_FILESAVE = os.getenv("ICP_SEQ_FILESAVE")
        ICP_BAD_SEQ_FILESAVE = os.getenv("ICP_BAD_SEQ_FILESAVE")

        icp_df.write_parquet(f"{os.getcwd()}/data/{ICP_SEQ_FILESAVE}")
        bad_df.write_parquet(f"{os.getcwd()}/data/{ICP_BAD_SEQ_FILESAVE}")
    
    return icp_df, bad_df

def by_mp(p_filepaths, w):

    # =*= mp begins here =*=
    good, bad = [], []

    with ThreadPoolExecutor(mp.cpu_count()-3) as pool:
        futs = {pool.submit(one_pq, f): f for f in p_filepaths}

        for fut in tqdm(as_completed(futs), total=len(futs), desc="pq"):
            g, b = fut.result()
            if g is not None:
                for g_sub in g:
                    good.append(g_sub)
            elif b is not None:
                bad.append(b)

    # =*= build dfs =*=
    icp_df = pl.from_dicts(good) if good else pl.DataFrame()
    bad_df = pl.from_dicts(bad) if bad else pl.DataFrame()
    os.makedirs(f"{os.getcwd()}/data", exist_ok=True)

    print(f"{len(good)} ok | {len(bad)} bad")

    if w:

        ICP_MP_FILESAVE = os.getenv("ICP_MP_FILESAVE")
        ICP_BAD_MP_FILESAVE = os.getenv("ICP_BAD_MP_FILESAVE")

        icp_df.write_parquet(f"{os.getcwd()}/data/{ICP_MP_FILESAVE}")
        bad_df.write_parquet(f"{os.getcwd()}/data/{ICP_BAD_MP_FILESAVE}")
    
    return icp_df, bad_df

# =*= helper functions =*=
def concat_dfs():

    # =*= assumes same column structures =*=
    DF_1 = os.getenv("DF_1")
    DF_2 = os.getenv("DF_2")
    DF_TOT = os.getenv("DF_TOT")

    df1 = pl.read_parquet(f"{os.getcwd()}/data/{DF_1}")
    df2 = pl.read_parquet(f"{os.getcwd()}/data/{DF_2}")
    df_tot = pl.concat([df1, df2])
    df_tot.write_parquet(f"{os.getcwd()}/data/{DF_TOT}")

    return df_tot

def dci_aggs(ICP_AGGS_FILE):
    icp_df = pl.read_parquet(f"{os.getcwd()}/data/{ICP_AGGS_FILE}")

    # =*= cast csns to str from int/list =*=
    CSNS = json.loads(os.getenv("CSNS"))
    icp_df = icp_df.with_columns(pl.col("csn").list.join(",").alias("csn")) # assume one csn per entry
    icp_df_csn = (icp_df.filter(pl.col("csn").is_in(CSNS)))
    icp_df_csn_tot = (icp_df
        .filter(pl.col("csn").is_in(CSNS))
        .group_by(by="csn")
            .agg(
                pl.col("file").alias("file"),
                pl.col("icp time").sum().alias("total icp time"),
                pl.len().alias("rows"),
                pl.col('point of care').list.join(",").alias("point of care")))
    
    return icp_df_csn, icp_df_csn_tot

def df_dedup(ICP_AGGS_FILE, get_dedup=True, get_dups=False):

    if get_dedup:
        icp_df = pl.read_parquet(f"{os.getcwd()}/data/{ICP_AGGS_FILE}")
        icp_df_dedup = icp_df.unique()
        ICP_AGGS_DEDUP_FILE = os.getenv("ICP_AGGS_DEDUP_FILE")
        print(f"removed {icp_df.height - icp_df_dedup.height}/{icp_df.height} duplicates")
        icp_df_dedup.write_parquet(f"{os.getcwd()}/data/{ICP_AGGS_DEDUP_FILE}")

        return icp_df_dedup

    # =*= get dups of any prior row =*=
    if get_dups:
        ICP_AGGS_DUP_FILE = os.getenv("ICP_AGGS_DUP_FILE")
        dedup_df = icp_df.is_duplicated()
        icp_df_dups = icp_df.filter(dedup_df)
        print(f"removed {icp_df.height - icp_df_dups.height}/{icp_df.height} duplicates")
        icp_df_dups.write_parquet(f"{os.getcwd()}/data/{ICP_AGGS_DUP_FILE}")

        return icp_df_dups

def icp_aggs(ICP_AGGS_FILE):

    # =*= count icp =*=
    tot_icp_df = pl.read_parquet(f"{os.getcwd()}/data/{ICP_AGGS_FILE}")
    tot_p = tot_icp_df.height
    icp_pass_cnt = tot_icp_df.filter(pl.col("include/exclude")=="pass").height
    set([j for i in tot_icp_df.filter(pl.col("include/exclude")=="pass")['point of care'] for j in i])
    print(f"{icp_pass_cnt}/{tot_p} icp files qualify")
    print(len(set([j for i in tot_icp_df.filter(pl.col("include/exclude")=="pass")['csn'] for j in i])))

def w_fn_to_fp(p_filepaths):

    FN_TO_FP_FILEPATH = os.getenv("FN_TO_FP_FILEPATH")

    # =*= adjust fn to fp =*=
    mac_os_fp = []
    for p in tot_icp_df["file"]:
        for f in p_filepaths:
            if f.endswith(p):
                mac_os_fp.append(f)

    mac_os_fp = pl.Series(mac_os_fp).alias("file")
    tot_icp_df = tot_icp_df.with_columns(mac_os_fp)
    tot_icp_df.write_parquet(f"{os.getcwd()}/data/{FN_TO_FP_FILEPATH}")

if __name__ == "__main__":

    # =*= init .env =*=
    dotenv.load_dotenv(f"{os.getcwd()}/config/.env")

    # =*= get filepaths iter =*=
    p_filepaths = format_fp(wavelabel_filt=None) # 1759 count: icp past 5.1.25
    # 10557: all past 5.1.25

    # =*= parallel method =*=
    # icp_df_mp, bad_df_mp = by_mp(p_filepaths, w=True) # set w=True to write

    # =*= sequential method =*=
    # icp_df_s, icp_df_seq = by_seq(p_filepaths[:5], w=True) # set w=True to write

    # =*= post-processing =*=
    # =*= check methods equal =*=
    # if icp_df_mp.sort(["csn","wavelabel"]).equals(icp_df_s.sort(["csn","wavelabel"])):
    #     print("dataframes by both methods (seq,mp) are equal")

    # =*= concat dfs (icp, bad) =*=
    # df_tot = concat_dfs()

    # =*= load generated icp aggs bedmaster scan =*=
    ICP_AGGS_FILE = os.getenv("ICP_AGGS_FILE")
    DCI_AGGS_FILESAVE = os.getenv("DCI_AGGS_FILESAVE")

    # =*= get aggregates from dci cohort by csns =*=
    # df_dedup(ICP_AGGS_FILE)
    # icp_aggs(ICP_AGGS_FILE)
    # icp_df_csn, icp_df_csn_tot = dci_aggs(ICP_AGGS_FILE)
    # icp_df_csn.write_parquet(f"{os.getcwd()}/data/{DCI_AGGS_FILESAVE}")

    # =*= replace "files" col with os filepaths =*=
    # w_fn_to_fp(p_filepaths)