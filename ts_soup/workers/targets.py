from ts_soup.common import BaseTarget,query_in_sql
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime


class TargetTable(BaseTarget):
    def __init__(self, tb, db: str = None, index_field: str = 'date', is_seperated=False, drop_axis=0,
                 drop_na_subset=None, drop_na_thresh=2, tag=None, has_unique_idx=False, user_def_pro: list = None):
        """
        目标表信息，如果该表需要分表，则tb传入没有分表年份的表名，年份在写入时会自动加入，同时is_seperated设置为True
        :param tb:表名，
        :param db:数据库，配置里的source_name，默认使用去向表第一个库
        :param index_field:
        :param is_seperated:是否是分表的表
        :param drop_axis:如果存在空数据，drop_na的轴
        :param drop_na_subset:需要考虑删除na的列（或行）
        :param tag:当使用data_updated_state表记录各表明细更新状况时，如果表名和状态表中字段名不一致，使用tag来记录状态表
        :param has_unique_idx:是否存在唯一索引或主键，以使用replace_into语句，默认关闭，需要表添加唯一索引或主键后才能开启
                否则会出现数据重复
        :param user_def_pro: 用户自定义其他属性,list类型，用以携带其他信息，在类中以self.[property]使用
        """
        super().__init__(tb, index_field, db, user_def_pro, drop_axis, drop_na_subset, drop_na_thresh, has_unique_idx,
                         is_seperated)
        self.tag = tag
        if is_seperated:
            self.__init_seperated_table()
        self.index_map = None

    def __init_seperated_table(self):
        """
        需要分表的表初始化建表  提前5天建立下年的分表
        :return:
        """
        now = datetime.datetime.now()
        next_ = now + relativedelta(days=5)
        if now.year+1==next_.year:
            self.db.execute(f'create table if not exists {self.tb+str(next_.year)} like {self.tb+str(now.year)} ')


    def build_output(self, cur_result):
        # 写库
        self.__execute_sql(cur_result)


    def __execute_sql(self,cur_result):
        """
        需要写库的操作
        :param cur_result:
        :return:
        """
        if self.is_seperated:
            """
            分表的处理
            """
            # 索引字段是时间类型格式的处理
            if str(cur_result[self.index_field].dtypes)=='datetime64' or str(cur_result[self.index_field].dtypes).lower() == 'object':

                # 将时间转为datetime64获取分表年份suffix
                cur_result[self.index_field] = pd.to_datetime(cur_result[self.index_field])
                date_suffix_list = cur_result[self.index_field].map(lambda x: x.strftime('%Y')).drop_duplicates().values.tolist()
                # 将时间转回 str
                cur_result[self.index_field] = cur_result[self.index_field].apply(lambda x: x.strftime('%Y-%m-%d'))
            else:
                # 不是时间格式的数据，碰到该情况再完善
                date_suffix_list = []

            #写库
            with self.db.begin() as conn:
                for date_suffix in date_suffix_list:
                    del_sql = f'delete from {self.tb + date_suffix} where {self.index_field} in ({query_in_sql(cur_result.loc[cur_result[self.index_field].str.startswith(date_suffix), self.index_field].drop_duplicates().values.tolist())})'
                    conn.execute(del_sql)
                    cur_result.loc[cur_result[self.index_field].str.startswith(date_suffix)].to_sql(
                        self.tb + date_suffix, con=conn, if_exists='append', index=False)
                # conn.execute(f'update data_updated_state set {self.tb} =1 where mkt_date in ({query_in_sql(cur_result[self.index_field].drop_duplicates().apply(lambda x:self.index_map[x] if self.index_map is not None else x).values.tolist())})')
                print(
                     f'{self.db_str}：{self.tb} 更新成功，更新日期为：\n{",".join(cur_result[self.index_field].drop_duplicates().apply(lambda x:self.index_map[x] if self.index_map is not None else x).values.tolist())}')
        else:
            """
            无需分表的处理
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
            print(f'{self.db_str}：{self.tb} 更新成功，更新日期为：\n{",".join(cur_result[self.index_field].drop_duplicates().apply(lambda x:self.index_map[x] if self.index_map is not None else x).values.tolist())}')












