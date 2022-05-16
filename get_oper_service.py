import contextlib
import io
import os
import sys
import pandas as pd
from requests.exceptions import HTTPError

from eDR.eDR_full import get_os_info as get_eDR_OS
from CDSEM.CDSEM_full import get_os_info as get_CDSEM_OS
import config as cfg

from support import get_SP_drive, save_file_to_SP_folder, send_failure_mail

@contextlib.contextmanager
def nostdout():
    save_stdout = sys.stdout
    sys.stdout = io.StringIO()
    yield
    sys.stdout = save_stdout


def save_to_archive(df, out_path, drive):

    time_columns = ["LotTurnDateTime", "LotCreateDT", "Start", "End"]

    try
        fl_item = drive.get_item_by_path(out_path)
    except HTTPError as e: # no such file
        status_code = e.response.status_code
        if status_code == 404:
            pass
        else:
            return("Got %d HTTP error while saving %s file" % (status_code, out_path))
    else:
        ftmp = io.BytesIO()
        fl_item.download(output=ftmp)
        ftmp.seek(0)
        dfa = pd.read_csv(ftmp)
        ftmp.close()
        for time_column in time_columns:
            dfa[time_column] = pd.to_datetime(dfa[time_column])

        df = dfa.append(df, sort=False)
        df = df.drop_duplicates(time_columns)
        df = df.sort_values("LotTurnDateTime")

    save_file_to_SP_folder(drive, out_path, df.to_csv(index=False).encode())

    return(None)

tools_to_process = {
    "eDR": get_eDR_OS,
    "CDSEM": get_CDSEM_OS
}


drive = get_SP_drive(cfg.SP_cfg)
if drive is None:
    print("Could not connect to SharePoint")
    send_failure_mail(cfg.failure_mail_recipients, "Could not connect to SharePoint, no extractions were made at all")
    sys.exit()

for tool, func in tools_to_process.items():
    print("Getting LTs for %s..." % tool)
    with nostdout():
        df = func()
    
    out_path_template = "%s<YEAR>/%s_%s_<YEAR>A.csv" % (cfg.SP_cfg["SharePoint_Path"][tool], cfg.fname_prefix, tool)
    
    min_year = df.LotTurnDateTime.min().year
    for year in range(min_year, cfg.current_year + 1):
        out_path = out_path_template.replace("<YEAR>", str(year))
        dfp = df.loc[df.LotTurnDateTime.dt.year == year]
        result = save_to_archive(dfp, out_path, drive)
        if result is not None:
            send_failure_mail(cfg.failure_mail_recipients, result)