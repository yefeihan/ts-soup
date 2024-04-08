import warnings
from abc import abstractmethod, ABC
from urllib import parse

import pandas as pd
from functools import wraps
import traceback

import pymysql
from sqlalchemy import create_engine

"""
"""

warnings.filterwarnings('ignore')

USABLE_DBS = {}

CURRENT_DATE = None
SYNC_FROM_DATE = None
EXECUTE_STATE = True
"""
记录每个表的更新状况的矩阵，全局对象
"""
DATA_UPDATED_STATE = None


def query_in_sql(list_):
    return ','.join(list(map(lambda x: '"' + x + '"', list_)))


def get_sqlalchemy_engine(db_info, db_type):
    global USABLE_DBS
    engine = create_engine(
        f'mysql+pymysql://{db_info["user"]}:{parse.quote_plus(db_info["pwd"])}@{db_info["ip"]}:3306/{db_info["db"]}')
    USABLE_DBS[db_info['alias']] = engine
    if db_info['default']:
        USABLE_DBS[f'{db_type}_default'] = engine


def get_pymsql_engine(db_info, db_type):
    global USABLE_DBS
    engine = pymysql.connect(
        host=db_info['ip'],
        user=db_info['user'],
        password=db_info['pwd'],
        port=int(db_info['port']),
        database=db_info['db']
    )
    USABLE_DBS[db_info['alias'] + '_pym'] = engine
    if db_info['default']:
        USABLE_DBS[f'{db_type}_default_pym'] = engine


def __init(customized_table, customized_time, sync_start, sync_end, db_infos):
    """
     1、指定时间，未指定表格:
        data_updated_state: 第四类转换完成的data_updated_state,对应时间的行 state列全部赋值为 0
        to_update_tables: 返回查询结果
     2、指定表格，未指定时间：
        data_updated_state: 第四类转换完成的data_updated_state
        to_update_tables: 返回指定表格
     3、指定时间，指定表格:
        data_updated_state: 第四类转换完成的data_updated_state，对应时间的行 state列全部赋值为 0
        to_update_tables: 返回指定表格
     4、未指定时间，未指定表格:
        data_updated_state: 转换成一个大的矩阵，列为日期，行为待同步表格，值为0和1，0表示未同步
        to_update_tables: 返回查询结果
    :param sync_end: 同步结束日期
    :param sync_start: 同步开始日期
    :param customized_table: 手动指定的表
    :param customized_time: 手动指定的时间
    :return:
    """
    global DATA_UPDATED_STATE, SYNC_FROM_DATE, CURRENT_DATE, USABLE_DBS

    for db_type in ['sources', 'targets']:  # 加载数据源配置 设置数据库连接
        for db_info in db_infos[db_type]:
            engine_types = db_info.get('engine_type')

            if engine_types:
                for engine_type in engine_types:
                    if engine_type == 'sqlalchemy':
                        get_sqlalchemy_engine(db_info, db_type)
                    else:
                        get_pymsql_engine(db_info, db_type)

            else:
                raise ValueError('未配置engine类型')

    SYNC_FROM_DATE = sync_start
    CURRENT_DATE = sync_end
    to_update_tables = pd.read_sql('select * from to_update_tables', con=USABLE_DBS['targets_default'])['table_name']
    with USABLE_DBS['targets_default'].begin() as conn:
        conn.execute("""
        CREATE TABLE if not exists `updated_state`  (
                      `update_date` date DEFAULT NULL,
                      `table_name` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
                      UNIQUE KEY `index` (`update_date`,`table_name`) USING BTREE COMMENT '唯一索引'
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """)

    data_updated_state = pd.read_sql('select * from updated_state where update_date >= "{}"'.format(SYNC_FROM_DATE),
                                     con=USABLE_DBS['targets_default'])

    data_updated_state['update_date'] = pd.to_datetime(data_updated_state['update_date'])  # 统一时间类型
    sync_date_range = pd.date_range(SYNC_FROM_DATE, CURRENT_DATE)
    fill_date_range = pd.DataFrame(sync_date_range, columns=['update_date'])
    fill_date_range['table_name'] = 'fill_date_range'
    # 利用fill_date_range把后面的Pivot table时间索引补全到 sync_from_date 开始到明天的日期
    data_updated_state = pd.concat([data_updated_state, fill_date_range])
    data_updated_state['state'] = 1
    pivot_table = data_updated_state.pivot_table(index='update_date', columns='table_name', values='state')

    both_columns = [i for i in to_update_tables if i in pivot_table.columns.values.tolist()]  # 原来记录里有且现在仍需要的表

    non_columns = [i for i in to_update_tables if i not in pivot_table.columns.values.tolist()]  # 原来记录没有现在新增的表
    state_table = pivot_table[both_columns].fillna(0)
    state_table[non_columns] = 0
    state_table = state_table.reset_index()
    state_table['update_date'] = state_table['update_date'].apply(lambda x: x.strftime('%Y-%m-%d'))

    if len(customized_time) > 0 and len(customized_table) == 0:  # 1、指定时间，未指定表格:
        DATA_UPDATED_STATE = state_table.loc[state_table['update_date'].isin(customized_time)]
        DATA_UPDATED_STATE.iloc[:, 1:] = 0
        return to_update_tables

    elif len(customized_time) == 0 and len(customized_table) > 0:  # 2、指定表格，未指定时间：
        DATA_UPDATED_STATE = state_table
        return customized_table

    elif len(customized_time) > 0 and len(customized_table) > 0:  # 3、指定时间，指定表格:
        DATA_UPDATED_STATE = state_table.loc[state_table['update_date'].isin(customized_time)]
        DATA_UPDATED_STATE.loc[:, customized_table] = 0
        return customized_table

    else:
        DATA_UPDATED_STATE = state_table  # 4、未指定时间，未指定表格:
        return to_update_tables


class BaseSource(ABC):
    def __init__(self,
                 db:str=None,
                 index_field: str = 'date',
                 empty_check: bool = True):
        """
        :param db: 数据库名
        :param index_field: 数据源筛选日期的字段，如果不提供则默认"date",如果需要查询全表，则index_field需要设置为 None。
        :param empty_check: 是否为当前方法主要数据源（除了配置信息的数据源），判空时使用，若不想使当前数据源参与判空，可设为 False
        """
        self.empty_check = empty_check
        if db:
            self.db = USABLE_DBS[db]
        else:
            self.db = USABLE_DBS['sources_default']
        self.index_field = index_field

    @abstractmethod
    def build_source(self, to_update_date):
        pass


class BaseTarget(ABC):
    def __init__(self,
                 tb,
                 index_field: str = 'date',
                 db: str = None,
                 user_def_pro: list = None,
                 drop_axis=0,
                 drop_na_subset=None,
                 drop_na_thresh=2,
                 has_unique_idx=False,
                 is_seperated=False
                 ):
        self.tb = tb
        self.user_def_pro = user_def_pro
        if user_def_pro is None:
            self.user_def_pro = []

        if db:
            self.db = USABLE_DBS[db]
            self.db_pym = USABLE_DBS[f'{db}_pym']
            self.db_str = db
        else:
            self.db = USABLE_DBS['targets_default']
            self.db_pym = USABLE_DBS['targets_default_pym']
            self.db_str = 'targets_default'

        self.is_seperated = is_seperated
        self.drop_axis = drop_axis
        self.drop_na_subset = drop_na_subset
        self.drop_na_thresh = drop_na_thresh
        self.has_unique_idx = has_unique_idx
        self.index_field = index_field

    @abstractmethod
    def build_output(self, cur_result):
        pass


class Executor:
    """
    数据处理原则：
        n2one: 数据源配置为is_data=True的对应日期查询结果只要存在一个全为空，则视当天的所有数据都为空，不更新数据表，也不更新操作记录表(updated_state)
        one2n:
               如果数据结果存在一个 None或者 emptyDataframe ，则当次调用 updated_state 表不更新
               如果数据结果全都有值，但是存在个别天数的行中存在空数据，则会删除空数据对应的行，updated_state更新取每个数据结果的交集作为日期
    """

    def __init__(self, sources, targets, executed_table):
        """
        数据库的操作器，负责从源表读取数据 _make_source_data，以及写入目标表 _handle_result
        """
        self.any_source_empty = False
        self.value = []
        self.sources = sources
        self.targets = targets
        self.source_data = []
        self.executed_table = executed_table
        self.to_update_date = self.__get_update_date()
        self.customized_updated_state = None


    def __get_update_date(self):
        return DATA_UPDATED_STATE.loc[~(DATA_UPDATED_STATE[self.executed_table] == 1), 'update_date']

    def make_source_data(self):
        """
        产生数据源source_data的方法，按照TargetInfo顺序写入 source_data中
        判断传入的主要数据源（除了配置信息）是存在空，如果存在空则视所有数据源都为空，当天数据未更新
        :return:
        """
        for source in self.sources:
            data = source.build_source(self.to_update_date)
            """
            判断数据源（empty_check设置为True的）是否存在空，如果存在空则视所有数据源都为空，当天数据未更新
            主要目的是为了避免需要频繁在funcs的处理逻辑中增加判空的判断，也可以手动给某数据源设置为False,如果某数据源
            empty_check设置为False，需要在数据处理逻辑中对data_source是否为空进行判断，不然当下文在进行数据处理时
            使用行列索引，如果df是空容易导致keyError错误。
            """
            if hasattr(source,'empty_check'):
                if source.empty_check and data.empty:
                    self.any_source_empty = True
                    for _ in self.targets:
                        self.value.append(None)
                    return
            else:
                raise ValueError("source.empty_check未设置")

            self.source_data.append(data)

    def handle_result(self):
        """
        处理结果的方法
        1.有分表的处理：
            将value和target_info顺序展开 ，得到对应处理完的df，根据df的日期取出分表的suffix，
            再根据suffix选择需要插入的数据进行数据插入，同时删除对应日期的数据
        2.无分表的处理：
            将value和target_info顺序展开 ，得到对应处理完的df，删除对应日期的数据，进行数据插入
        3.update_state处理：
            在依次遍历value时选择各数据目标df最小的索引集合（取交集），以使得多个数据目标时，如果多个表的对应
            索引值存在缺失，可以在下一次同步时进行同步。
        :return:
        """
        update_state_date = None
        func_update_flag = True
        for index, target in enumerate(self.targets):
            cur_result = self.value[index]

            if not isinstance(cur_result,pd.DataFrame):
                raise TypeError('返回数据类型不符')

            if len(target.user_def_pro) > 0:  # 获取在funcs里传入的额外属性（target未定义的）
                for pro in target.user_def_pro:
                    if hasattr(self,pro):
                        setattr(target, pro, getattr(self, pro))

            """
             处理返回值整个为none，如有一个返回结果为none，则整个方法的 这段to_update_date时间都应视为无数据，防止下文
             转str和合并索引时产生keyError
             """
            if cur_result is None:
                func_update_flag = False
                print(f'{target.tb} 无数据同步')
                continue

            # 处理返回值部分为空的情况
            params = {'axis': target.drop_axis, 'how': 'all', 'thresh': target.drop_na_thresh}
            if target.drop_na_subset:
                params['subset'] = target.drop_na_subset
            cur_result = cur_result.dropna(**params)

            # 处理经过dropna以后如果全为空的情况，视为全为空，原理同上
            if cur_result.empty:
                func_update_flag = False
                print(f'{target.tb} 无数据同步')
                continue

            """统一把index_field字段转为str类型，防止后面在insert_update_state 和各表插入数据时 使用datetime64 或 int等类型"""
            # datetime.date 在dataframe中有可能是Object类型
            if str(cur_result[target.index_field].dtypes) == 'datetime64' or str(
                    cur_result[target.index_field].dtypes).lower() == 'object':
                cur_result[target.index_field] = pd.to_datetime(cur_result[target.index_field]).apply(
                    lambda x: x.strftime('%Y-%m-%d'))
            else:
                # 碰到其他情况再继续完善，例如此处是inheritId，则把int或float转str
                cur_result[target.index_field] = cur_result[target.index_field].astype(str).apply(
                    lambda x: x.split('.')[0])

            # 调用统一接口完成成果产出
            target.build_output(cur_result)

            # 如果未传入自定义更新日期，则取数据日期作为 处理操作表的更新日期，取各数据结果交集
            if (update_state_date is not None) and (self.customized_updated_state is None):
                update_state_date = update_state_date.merge(
                    cur_result[[target.index_field]].drop_duplicates(),
                    left_on='update_date',
                    right_on=target.index_field,
                    how='inner'
                )[['update_date']]
            elif (update_state_date is None) and (self.customized_updated_state is None):
                update_state_date = pd.DataFrame(
                    cur_result[target.index_field].drop_duplicates().values,
                    columns=['update_date']
                )

        # 将方法的操作记录写入数据库
        if func_update_flag:
            if update_state_date is not None and not update_state_date.empty:
                self.__handle_insert_update_state(update_state_date)
            # 如果update_state_date无值，则说明传了customized值，更新日期在二者中选其一有值的
            else:
                if self.customized_updated_state is not None and not self.customized_updated_state.empty:
                    self.__handle_insert_update_state(self.customized_updated_state)

    def __handle_insert_update_state(self, update_state):
        """
        为update_state可能分表做准备
        :param update_state:
        :return:
        """
        update_state['table_name'] = self.executed_table
        with USABLE_DBS['targets_default'].begin() as conn:
            conn.execute(
                f'delete from updated_state where table_name = "{self.executed_table}" and update_date in ({query_in_sql(update_state["update_date"].values.tolist())})')
            update_state.to_sql('updated_state', con=conn, index=False, if_exists='append')


"""
数据库装饰器 按数据流向类型区分，向数据库写入数据
one2one:
表示数据仅从一张表导入另一张表
n2one:
表示数据从多张表取汇聚成一张表
one2n:
数据从一张表导出分多张表导入
n2n:
_seperate: 采用了分表策略的表
...
"""


def db_operator(sources: list, targets: list):
    """
    数据库操作器，包括源表与目标表，在funcs中需按照target_info顺序将结果添加到 executor.value中
    使用三个类来完成工作  1. Executor   方法层面，对应funcs模块中每个方法
                       2. Source 数据源层面，对应装饰器中定义的数据源
                       3. Target 目标表层面，对应装饰器中配置的目标表
    :param sources: 源表信息，list类型:
    [
        Source(db=源表所在库名(默认ALI_DATA),tb=源表名,query_field=查询源表字段名(默认*),date_field=源表日期字段名(默认'date'，如果查全表则为None)，other_condition=其他查询条件),
        Source(db=源表所在库名,tb=源表名,query_field=查询源表字段名,date_field=源表日期字段名，other_condition=其他查询条件) ...
    ]
    :param targets:目标表信息，list类型:
    [
        Target(db=目标数据库，tb=目标表名,date_field=目标表日期字段名),
        Target(db=目标数据库，tb=目标表名,date_field=目标表日期字段名) ...
    ]
    :return:
    """

    def wrapper(func):
        @wraps(func)
        def inner_wrapper():
            executor = None
            try:
                executor = Executor(targets=targets,
                                    sources=sources,
                                    executed_table=func.__name__)

                # 表示该方法已同步完所有数据，则不再执行后续操作
                if len(executor.to_update_date) == 0:
                    print(f'{func.__name__} 方法已同步至最新')
                    return

                if len(sources) != 0:
                    executor.make_source_data()

                # empty_check为True且有一天数据源为空则不执行数据处理函数
                if not executor.any_source_empty:
                    func(executor)

                executor.handle_result()
            except Exception:
                global EXECUTE_STATE
                EXECUTE_STATE = False
                print(executor.executed_table + '同步失败')
                print(traceback.format_exc())

        return inner_wrapper

    return wrapper


if __name__ == '__main__':
    pass
