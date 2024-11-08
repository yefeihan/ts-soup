import warnings
from abc import abstractmethod, ABC
import pandas as pd
from functools import wraps
import traceback
from sqlalchemy import text

warnings.filterwarnings('ignore')

# 注册形成的数据库连接
USABLE_DBS = {}

# 同步程序执行情况
EXECUTE_STATE = True

# 记录每个表的更新状况的矩阵，全局对象
DATA_UPDATED_STATE = None


def query_in_sql(list_):
    return ','.join(list(map(lambda x: '"' + x + '"', list_)))


def get_sqlalchemy_engine(db_info, db_type,engine_index):
    global USABLE_DBS
    engine = db_info['engine'][engine_index]
    USABLE_DBS[db_info['alias']] = engine
    if db_info['default']:
        USABLE_DBS[f'{db_type}_default'] = engine


def get_pymsql_engine(db_info, db_type,engine_index):
    global USABLE_DBS
    engine = db_info['engine'][engine_index]
    USABLE_DBS[db_info['alias'] + '_pym'] = engine
    if db_info['default']:
        USABLE_DBS[f'{db_type}_default_pym'] = engine


def __init(customized_table, customized_time, sync_start, sync_end, db_infos):
    """
    整个同步程序的初始化程序，并处理命令行参数 1、读取之前更新状态，形成updated_state矩阵。
                                        2、确定需要更新的table，并返回
    更新策略：
     1、指定时间，未指定表格:
        {指定时间内} {所有表}的内容全部从源重新读取并覆盖
     2、指定表格，未指定时间：
        {指定表}根据之前同步的记录，{未同步}的日期从源读取并写入目标
     3、指定时间，指定表格:
        {指定时间内} {指定表}的内容全部从源重新读取并覆盖
     4、未指定时间，未指定表格:
        {所有表} 根据 {未同步}的日期从源读取并写入
    :param sync_end: 同步结束日期
    :param sync_start: 同步开始日期
    :param customized_table: 手动指定的表
    :param customized_time: 手动指定的时间
    :return:
    """
    global DATA_UPDATED_STATE, USABLE_DBS
    # 加载数据源配置 设置数据库连接
    for db_type in ['sources', 'targets']:
        for db_info in db_infos[db_type]:
            engine_types = db_info.get('engine_type')

            if engine_types:
                for engine_index,engine_type in enumerate(engine_types):
                    if engine_type == 'sqlalchemy':
                        get_sqlalchemy_engine(db_info, db_type,engine_index)
                    else:
                        get_pymsql_engine(db_info, db_type,engine_index)

            else:
                raise ValueError('未配置engine类型')

    # 创建数据表 updated_state
    with USABLE_DBS['targets_default'].begin() as conn:
        conn.execute(text("""
        CREATE TABLE if not exists `updated_state`  (
                      `update_date` date DEFAULT NULL,
                      `table_name` varchar(50) COLLATE utf8mb4_general_ci NOT NULL,
                      UNIQUE KEY `index` (`update_date`,`table_name`) USING BTREE COMMENT '唯一索引'
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """))

    # 需要同步的表，根据数据库中to_update_tables确定，如果传入--table，则为传入的表
    to_update_tables = customized_table if len(customized_table) > 0 else pd.read_sql('select * from to_update_tables', con=USABLE_DBS['targets_default'])['table_name']
    # 如果传入--time，则将传入日期的，需要更新的表的状态全部设置为0，全部进行同步
    if len(customized_time) > 0:
        DATA_UPDATED_STATE = pd.DataFrame(index=customized_time,columns=to_update_tables)
        DATA_UPDATED_STATE = DATA_UPDATED_STATE.reset_index().rename({'index':'update_date'},axis=1).fillna(0)
        return to_update_tables

    # 以下按照updated_state记录情况进行同步
    data_updated_state = pd.read_sql('select * from updated_state where update_date >= "{}" and update_date <= "{}"'.format(sync_start,sync_end),
                                     con=USABLE_DBS['targets_default'])
    # 统一时间类型
    data_updated_state['update_date'] = pd.to_datetime(data_updated_state['update_date'])
    sync_date_range = pd.date_range(sync_start, sync_end)
    fill_date_range = pd.DataFrame(sync_date_range, columns=['update_date'])
    fill_date_range['table_name'] = 'fill_date_range'
    # 利用fill_date_range把data_updated_state的日期补全为整个需要同步的期间的所有日期
    # 透视的时候如果某table_name没有对应同步日期记录，则该日期table_name的值为NA
    data_updated_state = pd.concat([data_updated_state, fill_date_range])
    data_updated_state['state'] = 1
    pivot_table = data_updated_state.pivot_table(index='update_date', columns='table_name', values='state')
    # 原来记录里有且现在仍需要的表
    both_columns = [i for i in to_update_tables if i in pivot_table.columns.values.tolist()]
    # 原来记录没有现在新增的表，新增的表需要扩展data_updated_state
    non_columns = [i for i in to_update_tables if i not in pivot_table.columns.values.tolist()]

    # DATA_UPDATED_STATE是data_updated_state的二维表形式，以日期为行索引，表名为列索引，
    # 且经过non_columns的扩展，是最终同步时判断状态所依赖的表

    # 把原来有的表data_updated_state的记录，未同步的日期置为0
    DATA_UPDATED_STATE = pivot_table[both_columns].fillna(0)
    # 扩展新增表，扩展DATA_UPDATED_STATE
    DATA_UPDATED_STATE[non_columns] = 0
    DATA_UPDATED_STATE = DATA_UPDATED_STATE.reset_index()
    DATA_UPDATED_STATE['update_date'] = DATA_UPDATED_STATE['update_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    return to_update_tables


class BaseSource(ABC):
    """
    定义需要使用在入口函数中注册的数据库连接的source的基类，即 如果需要使用数据库连接，则需要继承BaseSource
    """

    def __init__(self,
                 db: str = None,
                 index_field: str = 'date',
                 empty_check: bool = True):
        """
        :param db: 数据库名
        :param index_field: 数据源筛选日期的字段，如果不提供则默认"date",如果需要查询全表，则index_field需要设置为 None。
        :param empty_check: 是否为当前方法主要数据源（除了配置信息的数据源），判空时使用，若不想使当前数据源参与判空，可设为 False
        """
        self.empty_check = empty_check
        if db:
            self.db = USABLE_DBS.get(db)
        else:
            self.db = USABLE_DBS.get('sources_default')
        self.index_field = index_field

    @abstractmethod
    def build_source(self, to_update_date):
        """
        :param to_update_date:
        :return: yyyy-mm-dd 格式日期列表
        """
        pass


class BaseTarget(ABC):
    """
    定义需要使用在入口函数中注册的数据库连接的target的基类，即 如果需要使用数据库连接，则需要继承BaseTarget
    """

    def __init__(self, db: str = None):
        if db:
            self.db = USABLE_DBS.get(db)
            self.db_pym = USABLE_DBS.get(f'{db}_pym')
            self.db_str = db
        else:
            self.db = USABLE_DBS.get('targets_default')
            self.db_pym = USABLE_DBS.get('targets_default_pym')
            self.db_str = 'targets_default'

    @abstractmethod
    def build_output(self, cur_result):
        """
        :param to_update_date:
        :return: yyyy-mm-dd 格式日期列表
        """
        pass


class Executor:
    """
    数据处理原则：
        n2one: 数据源配置为is_data=True的对应日期查询结果只要存在一个全为空，则视当天的所有数据都为空，不更新数据表，也不更新操作记录表(updated_state)
        one2n:
               如果数据结果存在一个 None或者 emptyDataframe ，则当次调用 updated_state 表不更新
               如果数据结果全都有值，但是存在个别天数的行中存在空数据，则会删除空数据对应的行，updated_state更新取每个数据结果的交集作为日期
    """

    def __init__(self, sources, targets, executed_table, customized_updated_state=None):
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
            if hasattr(source, 'empty_check'):
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
        调用各target处理对应返回结果的方法，并取得各target执行后需要更新的日期，updated_date
        update_state处理：
            在依次遍历value时选择各数据目标df最小的日期集合（取交集），以使得多个target时，如果多个target的对应
            日期缺失，则updated_state依旧为0 。
        :return:
        """
        update_state_date = None
        func_update_flag = True
        for index, target in enumerate(self.targets):
            cur_result = self.value[index]

            """
             处理返回值整个为none，如有一个返回结果为none，则整个方法的 这段to_update_date时间都应视为无数据，防止下文
             转str和合并索引时产生keyError
             """
            if cur_result is None:
                func_update_flag = False
                print(f'{target.tb} 无数据同步，返回结果为None')
                continue

            # 调用统一接口完成成果产出，返回每个target更新的日期，最后取交集作为该func的完成同步的日期
            # 先统一转datetime，再转str
            updated_date = target.build_output(cur_result)
            if updated_date is None:
                continue
            date_df = pd.to_datetime(updated_date)
            updated_date = pd.DataFrame(date_df, columns=['update_date']).drop_duplicates().applymap(
                lambda x: x.strftime('%Y-%m-%d'))

            # 如果未传入自定义更新日期，则取数据日期作为 处理操作表的更新日期，取各数据结果交集
            if update_state_date is not None:
                update_state_date = update_state_date.merge(
                    pd.DataFrame(updated_date, columns=['update_date']).drop_duplicates(),
                    on='update_date',
                    how='inner'
                )[['update_date']]
            else:
                update_state_date = pd.DataFrame(updated_date, columns=['update_date']).drop_duplicates()

        # 将方法的操作记录写入数据库
        if func_update_flag:
            if update_state_date is not None and not update_state_date.empty:
                self.__handle_insert_update_state(update_state_date)

    def __handle_insert_update_state(self, update_state):
        update_state['table_name'] = self.executed_table
        with USABLE_DBS['targets_default'].begin() as conn:
            conn.execute(
                f'delete from updated_state where table_name = "{self.executed_table}" and update_date in ({query_in_sql(update_state["update_date"].values.tolist())})')
            update_state.to_sql('updated_state', con=conn, index=False, if_exists='append')


def db_operator(sources: list, targets: list):
    """
    数据库操作器，包括源表与目标表，在funcs中需按照target_info顺序将结果添加到 executor.value中
    使用三个类来完成工作  1. Executor   方法层面，对应funcs模块中每个方法
                       2. Source 数据源层面，对应装饰器中定义的数据源
                       3. Target 目标表层面，对应装饰器中配置的目标表
    :param sources: 源表信息，list类型:
    [
        Source(db=源表所在库名(默认ALI_DATA),tb=源表名,...),
        Source(db=源表所在库名,tb=源表名,...) ...
    ]
    :param targets:目标表信息，list类型:
    [
        Target(db=目标数据库，tb=目标表名,...),
        Target(db=目标数据库，tb=目标表名,....) ...
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
                    print(f'{func.__name__} 方法已同步至最新\n\n')
                    return

                msg_len = len(f' {func.__name__} 开始 ')
                before_white_len = int((110-msg_len)/2)
                after_white_len = 110-msg_len-before_white_len

                print('>'*before_white_len + f' {func.__name__} 开始 ' +'>'*after_white_len)
                if len(sources) != 0:
                    executor.make_source_data()

                # empty_check为True且有一天数据源为空则不执行数据处理函数
                if not executor.any_source_empty:
                    func(executor)

                executor.handle_result()
                print('<'*before_white_len+ f' {func.__name__} 结束 '+'<'*after_white_len+'\n'*2)
            except Exception:
                global EXECUTE_STATE
                EXECUTE_STATE = False
                print(executor.executed_table + '同步失败')
                print(traceback.format_exc())

        return inner_wrapper

    return wrapper


if __name__ == '__main__':
    pass
