import pandas as pd
from ts_soup.common import query_in_sql,BaseSource


class Source(BaseSource):
    def __init__(self,
                 tb: str,
                 db: str = None,
                 empty_check=True,
                 query_field: str = '*',
                 index_field='date',
                 other_condition: str = None):
        """
        单表查询数据源信息
        :param tb: 数据表名
        :param db: 数据库名
        :param query_field:要查询的字段，默认为 *
        :param index_field: 日期字段，如果不提供则默认"date",如果需要查询全表，则index_field需要设置为 None。
        :param other_condition: 其他查询条件，直接拼接到 from table 后，覆盖date_field
        :param empty_check: 是否为当前方法主要数据源（除了配置信息的数据源），判空时使用，若不想使当前数据源参与判空，可设为 False
        """
        super().__init__(db, index_field, empty_check)
        self.tb = tb
        self.query_field = query_field
        self.other_condition = other_condition


    def build_source(self, to_update_date):
        base_sql = f'select {self.query_field} from {self.tb} where 1=1 '
        if self.other_condition:
            base_sql += f' and {self.other_condition} '
        if self.index_field:
            base_sql += f' and {self.index_field} in ({query_in_sql(to_update_date.values.tolist())}) order by {self.index_field}'
        return pd.read_sql(base_sql, con=self.db)


class MultiSource(BaseSource):
    """
    多数据源联合查询时使用
    """
    def __init__(self,
                 relations: list,
                 db: str=None ,
                 empty_check=True,
                 index_field='date'):
        """
        :param relations:连接条件模板如下：
        {'left':'','right':'','l_on':'','r_on':'','how':'','lquery_field':'','rquery_field':' ','l_cons':'','r_cons':''},
        :param db:
        :param empty_check:
        :param index_field:
        """
        super().__init__(db, index_field, empty_check)
        self.relations = relations

    def build_source(self, to_update_date):
        query_params = []
        join_stm = ''
        where_clause = ''
        for index,rel in enumerate(self.relations):
            query_params.append(','.join([f'{rel["left"]}.'+i if i!='' else '' for i in rel['lquery_field'].split(',')])\
                            +(',' if rel['lquery_field']!='' else '')+\
                            ','.join([f'{rel["right"]}.'+i if i!='' else '' for i in rel['rquery_field'].split(',')]))
            join_stm+=f"{rel['how']} join {rel['right']} on {rel['left']}.{rel['l_on']}={rel['right']}.{rel['r_on']} "
            where_clause += (f" and {rel['left']+'.'+rel['l_cons']}" if rel['l_cons']!='' else '')+(f"and {rel['right']+'.'+rel['r_cons']}" if rel['r_cons']!='' else '')
        base_sql = f'select {",".join(query_params)} from {self.relations[0]["left"]} {join_stm} where 1=1 {where_clause} and {self.index_field} in ({query_in_sql(to_update_date)})'
        return pd.read_sql(base_sql,con=self.db)


class RawSqlSource(BaseSource):
    def __init__(self,  index_field,sql,db:str=None, empty_check=True):
        """
        直接使用sql的源
        :param db:
        :param index_field:
        :param empty_check:
        :param sql:
        """
        super().__init__(db, index_field, empty_check)
        self.sql = sql
    def build_source(self, to_update_date):
        return pd.read_sql(self.sql.format(query_in_sql(to_update_date)),con=self.db)
