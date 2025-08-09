# capsule-icp-tracker

###

_Polars_-based approach to get attributes from **Capsule** _w-_ .parquet files after 5.1.25. Code is entirely custom towards Capsule directory and file structure.

Focused on:
1. identifying intracranial pressure (ICP) waveform measurements via clinical thresholding (0-25 mmHg);
2. assessing site data acquisition via file patient location analytics.

## Table of contents
[Prerequisites](#prerequisites)

[Setup](#setup)

[Usage](#usage)

[Project Structure](#project-structure)

[Data Requirements](#data-requirements)

[Development](#development)

[Contributing](#contributing)

[License](#license)

[Acknowledgements](#acknowledgements)

[Contact](#contact)


## Prerequisites
1. _Python_ 3.13+

2. _Polars_ 1.31+

3. [Philips **Capsule**](https://capsuletech.com/) network drive. The code has been tested and validated on Capsule at MGB, and should be tuned to each site.

## Setup
1. Clone this repo: ```git clone https://github.com/MGB-NeuroAI-Center/capsule-icp-tracker.git```
2. Build environment with: ```mamba env create -f environment.yml``` located in ```config/```. Adjust prefix in .yml to desired user path.

## Usage
Ensure path access to ```wavelabel_counts.csv```

Specify variable paths in .env:
- drive prefix to search Capsule network drive
- filesaves for aggregated Capsule files

Run main ```icp.py``` or plot helper ```plot.py```

### Common use cases
1. Multiprocessing approach to scan Capsule -w files:
```
icp_df_mp, bad_df_mp = by_mp(p_filepaths, w=True) # set w=True to write
```

2. Sequential approach to scan Capsule -w files (primarily for tuning):
```
icp_df_s, icp_df_seq = by_seq(p_filepaths[:5], w=True) # set w=True to write
```

3. Process Capsule files in batches:
```
df_tot = concat_dfs() # must specify DF_1, DF_2, and DF_TOT in .env
```

4. Deduplicate rows:
```
df_dedup(ICP_AGGS_FILE) # saves to ICP_AGGS_DEDUP_FILE specified in .env
```

5. Gather analytics from curated Polars dataframe of processed Capsule files:
```
ICP_AGGS_FILE = os.getenv("ICP_AGGS_FILE")
icp_df_csn, icp_df_csn_tot = dci_aggs(ICP_AGGS_FILE) # must specify CSNs list in .env
```

## Project structure
```
capsule-icp-tracker/
├── src/                                # source code
│   ├── icp.py                          # Polars-based approach for aggregating Capsule files
│   └── plot.py                         # plot data from select Capsule files
├── data/
│   └── wavelabel_counts.csv            # sheet of Capsule files and their available waveform label types
├── config/                             # configuration files
│   ├── .env.example                    # template for script arguments
│   ├── .env                            # environment variables
│   └── environment.yml                 # template for packages setup
├── README.md                           # this file
├── CHANGELOG.md                        # version history
├── LICENSE                             # license info
└── .gitignore                          # Git ignore rules
```

## Configuration
Refer to ```config/env.example```

## Data requirements
**Input**:
- wavelabel_counts.csv: must a .csv of three columns: 'label' for measurement type, 'count' for number of rows in that file, 'file' for filepath.

|label|count|file|
|-|-|-|
|waveform type ("Unspecified Invasive Blood Pressure Waveform (P)")|row count (2025)|/capsule-volume/folder/file.parquet|

- filesave locations (set in .env)

**Output**:

Generates .parquet of these attributes:

        result = {
            "csn": _unique_ contact serial number for patient encounter,
            "start time": starting neuron-time of current file,
            "end time": ending measurement-time of current file,
            "icp time": measured ICP duration based on span of 'neuron-time' and 'measurement-time' difference AND average falling within 0-25 mmHg per message (row),
            "device attribute": device id uniquely listed in current file,
            "device id": device version uniquly listed in current file,
            "point of care": health service location,
            "facility": facility department,
            "room": room,
            "bed": bed,
            "channel ids": available channel ids,
            "channel means": average of means taken across each message per channel,
            "channel medians": average of medians taken across each message per channel,
            "channel mins": minimum value per channel,
            "channel maxs": maximum value per channel,
            "channel rows": count of rows per channel,
            "include/exclude": evaluation, 'pass' or 'mean_crit_unment', of icp based on 0-25 mmHg threshold,
            "file": filepath of that os
           }

## Development
Reach out to project maintainer for guidelines and data access.

## Contributing
1. Fork the repository
2. Create feature branch (```git checkout -b feature/new-branch```)
3. Commit changes (```git commit -m "add new feature"```)
4. Push to branch (```git push origin feature/new-feature```)
5. Open Pull Request (*PR*)

## License
This project is licensed under the **Attribution Non-Commercial 4.0 International License** - see the LICENSE file for details.

Copyright (c) 2025 The General Hospital Corporation

## Acknowledgements
Thank you to all those in the NeuroAI Center who contributed to this project.

## Contact
- Maintainer/Issues: Jackie at _jale8[at]mgh[dot]harvard[dot]edu_ with any questions and feedback!
- Team: MGB NeuroAI Center at _mgb[dot]neuroai[at]partnershealthcare[dot]onmicrosoft[dot]com_
---
Last updated: 8.5.25 | Version: 1.0