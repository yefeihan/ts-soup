from ts_soup.common import BaseTarget, query_in_sql
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime


class TargetTable(BaseTarget):
    def __init__(self, tb,
                 db: str = None,
                 index_field: str = 'date',
                 drop_axis=0,
                 drop_na_subset=None,
                 drop_na_thresh=None,
                 has_unique_idx=False,
                 is_empty_effect=True
                 ):
        """
        目标表信息，如果该表需要分表，则tb传入没有分表年份的表名，年份在写入时会自动加入，同时is_seperated设置为True
        :param tb:表名，
        :param db:数据库
        :param index_field:作为索引的字段，一般为日期
        :param drop_axis:如果存在空数据，drop_na的轴
        :param drop_na_subset:需要考虑删除na的列（或行）
        :param drop_na_thresh:如果该轴不为nan的值的个数少于 drop_na_thresh设置的值，进行dropna操作
        :param has_unique_idx:是否存在唯一索引或主键，以使用replace_into语句，默认关闭，需要表添加唯一索引或主键后才能开启
                否则会出现数据重复
        :param is_empty_effect:表示该target  数据为空 是否影响update_state设置为 1 ,True表示数据为空，不能设置为 1
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
        self.is_empty_effect = is_empty_effect

    def build_output(self, cur_result):
        # 写库

        # is_empty_effect 为True，表示如果该target数据为空，需要在以后再同步，因此当天update_state 不能设置为1
        # 返回none则不会进行交集，因此不影响其他target update_state设置完成情况
        if cur_result.empty:
            print(f'{self.tb} 无数据同步')
            return [] if self.is_empty_effect else None

        # 统一把index_field字段转为str类型，防止后面在insert_update_state 和各表插入数据时 使用datetime64 或 int等类型
        # datetime.date 在dataframe中有可能是Object类型
        if str(cur_result[self.index_field].dtypes) == 'datetime64' or str(
                cur_result[self.index_field].dtypes).lower() == 'object':
            cur_result[self.index_field] = pd.to_datetime(cur_result[self.index_field]).apply(
                lambda x: x.strftime('%Y-%m-%d'))

        self.__execute_sql(cur_result)

        """如果是多个数据源合并到一个表，多个字段有若干个为空，或者一个日期内多行内有字段为空，
        则需要在插入前删除为空的字段的日期，保证下次执行还能同步该日期
        可以通过 drop_na_subset,drop_na_thresh 参数控制是否执行该步骤，若是因此而被删除的日期，
        在同步日志中会有 "因数据不全(多个字段有至少一个字段为Null)导致update_state当天日期未更新" 提示
        # e.g  
        pdate       period  value1  value2   
        2023-01     1       2       na
        2023-01     1       na      3
        2023-02     1       3       4
        2023-01日期有Na，则不作为更新完成的日期
        """
        params = {'axis': self.drop_axis}
        if self.drop_na_subset:
            params['subset'] = self.drop_na_subset
        if self.drop_na_thresh:
            params['thresh'] = self.drop_na_thresh

        updated_date = []
        not_updated_date = []
        groups = cur_result.groupby(by=self.index_field, dropna=False)
        for index, group in groups:
            rows = group.shape[0]
            new_df = group.dropna(**params)
            new_rows = new_df.shape[0]
            if rows == new_rows:
                updated_date.append(index)
            else:
                not_updated_date.append(index)
        if len(not_updated_date) != 0:
            print(f'{self.tb} 因数据不全(多个字段有至少一个字段为Null)导致update_state当天日期未更新：\n{",".join(sorted(not_updated_date, reverse=True))}')
        return updated_date if self.is_empty_effect else None

    def __execute_sql(self, cur_result):
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
                cursor.executemany(
                    f'replace into {self.tb} ({",".join(insert_field)}) values ({",".join(["%s" for _ in range(len(insert_field))])})',
                    cur_result.values.tolist())
                self.db_pym.commit()
            except Exception:
                import traceback
                print(traceback.format_exc())
                self.db_pym.rollback()
                raise Exception('数据库操作错误!')
        else:
            with self.db.begin() as conn:
                conn.execute(
                    f'delete from {self.tb} where {self.index_field} in ({query_in_sql(cur_result[self.index_field].drop_duplicates().values.tolist())})')
                cur_result.to_sql(self.tb, con=conn, if_exists='append', index=False)
        print(
            f'{self.tb} 更新成功，更新日期为：\n{",".join(cur_result[self.index_field].drop_duplicates().sort_values(ascending=False).values.tolist())}')
