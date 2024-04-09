from ts_soup.common import BaseTarget,query_in_sql
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime


class TargetTable(BaseTarget):
    def __init__(self, tb,
                 db: str = None,
                 index_field: str = 'date',
                 drop_axis=0,
                 drop_na_subset=None,
                 drop_na_thresh=2,
                 has_unique_idx=False
                 ):
        """
        目标表信息，如果该表需要分表，则tb传入没有分表年份的表名，年份在写入时会自动加入，同时is_seperated设置为True
        :param tb:表名，
        :param db:数据库
        :param index_field:作为索引的字段，一般为日期
        :param drop_axis:如果存在空数据，drop_na的轴
        :param drop_na_subset:需要考虑删除na的列（或行）
        :param has_unique_idx:是否存在唯一索引或主键，以使用replace_into语句，默认关闭，需要表添加唯一索引或主键后才能开启
                否则会出现数据重复
        """
        super().__init__(
                         db,
                         )
        self.tb = tb
        self.index_field = index_field
        self.drop_axis = drop_axis
        self.drop_na_subset = drop_na_subset
        self.drop_na_thresh = drop_na_thresh
        self.has_unique_idx = has_unique_idx


    def build_output(self, cur_result):
        # 写库
        # 处理返回值部分为空的情况
        params = {'axis': self.drop_axis, 'how': 'all', 'thresh': self.drop_na_thresh}
        if self.drop_na_subset:
            params['subset'] = self.drop_na_subset
        cur_result = cur_result.dropna(**params)

        # 处理经过dropna以后如果全为空的情况，视为全为空，原理同上
        if cur_result.empty:
            print(f'{self.tb} 无数据同步')
            return

        """统一把index_field字段转为str类型，防止后面在insert_update_state 和各表插入数据时 使用datetime64 或 int等类型"""
        # datetime.date 在dataframe中有可能是Object类型
        if str(cur_result[self.index_field].dtypes) == 'datetime64' or str(
                cur_result[self.index_field].dtypes).lower() == 'object':
            cur_result[self.index_field] = pd.to_datetime(cur_result[self.index_field]).apply(
                lambda x: x.strftime('%Y-%m-%d'))

        self.__execute_sql(cur_result)
        return cur_result[self.index_field].values.tolist()


    def __execute_sql(self,cur_result):
        """
        需要写库的操作
        :param cur_result:
        :return:
        """
        # 设置了使用replace_into语句(多重索引的情况下无法使用sqlalchemy)，此时需要表设置唯一索引或者主键
        if self.has_unique_idx:
            # 使用Pymysql的连接
            insert_field = cur_result.columns.values.tolist()
            cursor = self.db_pym.cursor()
            try:
                cursor.executemany(f'replace into {self.tb} ({",".join(insert_field)}) values ({",".join(["%s" for _ in range(len(insert_field))])})',cur_result.values.tolist())
                self.db_pym.commit()
            except Exception:
                import traceback
                print(traceback.format_exc())
                self.db_pym.rollback()
                raise Exception('数据库操作错误!')
        else:
            with self.db.begin() as conn:
                conn.execute(f'delete from {self.tb} where {self.index_field} in ({query_in_sql(cur_result[self.index_field].values.tolist())})')
                cur_result.to_sql(self.tb, con=conn, if_exists='append', index=False)
        print(f'{self.db_str}：{self.tb} 更新成功，更新日期为：\n{",".join(cur_result[self.index_field].drop_duplicates().values.tolist())}')









