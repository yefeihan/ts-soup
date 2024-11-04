import argparse
import datetime
import importlib
import os
import sys
from typing import Optional
from dateutil.relativedelta import relativedelta

parser = argparse.ArgumentParser()
parser.add_argument('--table', default=[], nargs='+')
parser.add_argument('--time', default=[], nargs='+')
args = parser.parse_args()

from ts_soup.common import __init


def run_sync(db_infos: dict,
             sync_start_from: Optional[dict] = None,
             sync_end:Optional[datetime.datetime] = None,
             sync_delay: int = 1,
             funcs_file: str = 'funcs'
             ):
    sync_start_from = sync_start_from or {'months': 3}
    sync_end = sync_end or (datetime.datetime.now() + relativedelta(days=sync_delay))
    sync_start = (sync_end - relativedelta(**sync_start_from)).strftime('%Y-%m-%d')
    sync_end = sync_end.strftime('%Y-%m-%d')
    to_update_tables = __init(args.table, args.time, sync_start, sync_end, db_infos)

    cwd = os.getcwd()  # 手动导包
    sys.path.append(os.path.join(cwd, funcs_file))
    modules = []
    for file in os.listdir(os.path.join(cwd, funcs_file)):
        modules.append(importlib.import_module(file.split('.')[0]))

    for table in to_update_tables:  # 执行funcs_name下模块的方法
        for module in modules:
            if table in dir(module):
                getattr(module, table)()

    from ts_soup.common import EXECUTE_STATE  # 检测是否异常
    if not EXECUTE_STATE:
        raise Exception("同步异常")


if __name__ == '__main__':
    pass
