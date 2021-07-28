#!/usr/bin/env python
# -*- coding:utf8 -*-
"""
:Copyright: 2021, BBD Tech. Co.,Ltd.
:Description:
:Author: zhouxiaohui@bbdservice.com
:Date: 2021-05-11 10:22
"""
import json
import logging
import os
import re
import traceback
import typing

from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

from whetstone.common.exception import FriendlyException
from whetstone.core.entry import Entry
from whetstone.utils.index import index_join_merge
from whetstone.utils.shell import Shell


class Parameter(dict):
    pass


IndexResult = typing.Dict[str, bool]


class ResultManager:

    def __init__(self, result_path, load=False, logger: logging.Logger = None):
        self._index_result = {}
        self._logger = logger or logging.getLogger()
        self._result_path = result_path
        self._load() if load else None

    @property
    def json(self):
        return self._index_result

    def update(self, key, status):
        self._index_result[key] = status

    def _load(self) -> IndexResult:
        if self._result_path and os.path.exists(self._result_path):
            with open(self._result_path, mode="r") as fp:
                try:
                    self._index_result = json.load(fp) or {}
                    return self._index_result
                except:
                    self._logger.warning(f"load last result error:{traceback.format_exc()}")

    def dump(self, result: IndexResult = None):
        self._index_result = (result or self._index_result)
        if self._result_path:
            if not os.path.exists(self._result_path):
                os.makedirs(os.path.dirname(self._result_path), exist_ok=True)
            with open(self._result_path, mode="w") as fp:
                try:
                    json.dump(self._index_result, fp)
                except:
                    self._logger.warning(f"dump last result error:{traceback.format_exc()}")


class View:
    def __init__(self, name: str, desc: str, persistent: str, persistent_param: typing.Dict,
                 cache: bool, cache_level: StorageLevel,
                 dependencies: typing.List[str],
                 df_func: typing.Union[typing.Callable[..., DataFrame], DataFrame]):
        self._name = name
        self._desc = desc
        self._persistent = persistent
        self._persistent_param = persistent_param or {}
        self._cache_level = cache_level
        self._cache = cache
        self._dependencies = dependencies or []
        self._dependencies.remove(self._name) if self._name in self._dependencies else None
        self._df_func = df_func
        self._calculated = False
        self._inner_df = None

    @property
    def rst_df(self):
        return self._inner_df

    @property
    def dependencies(self):
        return self._dependencies

    def execute(self, entry: Entry, param: Parameter, rst_mgr: ResultManager, logger: logging.Logger):
        if ((not entry.fresh) or (entry.fresh and self._calculated)) \
                and rst_mgr.json.get(self._name) \
                and check_index_hdfs_data_exists(entry, self._name, logger):
            logger.info(f"index {self._name} last result is True and hdfs data exist, just read it to df")
            df = getattr(entry.spark.read, self._persistent)(get_index_hdfs_path(entry, self._name))
            df.createOrReplaceTempView(self._name)
            self._inner_df = df
        else:
            if not self._calculated:
                logger.info(f"index {self._name} df executing")
                if isinstance(self._df_func, DataFrame):
                    df = self._df_func
                else:
                    df = self._df_func(entry, param, logger)
                self._inner_df = df
                # TODO just for unit test, should uncomment it for product
                # df.show()
                df.createOrReplaceTempView(self._name)
                if self._persistent:
                    getattr(df.write, self._persistent)(get_index_hdfs_path(entry, self._name),
                                                        **self._persistent_param)
                if self._cache:
                    df.persist(self._cache_level or StorageLevel.MEMORY_ONLY)
            else:
                logger.info(f"index {self._name} df executed before, then skip")
        rst_mgr.update(self._name, True)
        self._calculated = True
        rst_mgr.dump()
        return True


class SQL:
    def __init__(self, statement, alias):
        self._statement = statement
        self._alias = alias
        if not self._statement:
            raise FriendlyException(f"SQL {self._alias} should be not empty statement")

    @property
    def alias(self):
        return self._alias

    def execute(self, entry: Entry, param: Parameter, logger: logging.Logger) -> DataFrame:
        df = entry.spark.sql(self._statement)
        # df.createOrReplaceTempView(self.alias)
        return df


class Index:

    def __init__(self, name, *sqls):
        self._sqls = sqls
        self._name = name
        if not len(self._sqls) > 0:
            raise FriendlyException(f"index {name} should contains one sql statement at least")
        if not self._name:
            raise FriendlyException(f"index should be assigned a index code name")

    @property
    def name(self):
        return self._name

    def execute(self, entry: Entry, param: Parameter, logger: logging.Logger) -> DataFrame:
        for sql in self._sqls[:-1]:
            df = sql.execute(entry, param, logger)
            df.createOrReplaceTempView(sql.alias)
            # TODO just for unit test, should uncomment it for product
            # df.show()
        last_sql_statement = self._sqls[-1]
        df = last_sql_statement.execute(entry, param, logger)
        # TODO just for unit test, should uncomment it for product
        # df.show()
        return df


def check_cycle_dependencies(views: typing.Dict[str, View]):
    pass


def get_index_hdfs_path(entry: Entry, name: str):
    # make sure use the result template hdfs path format '{hdfs_root_path}/{version}/result/index'
    # if index_hdfs_path not exists
    if name.startswith("_tmp_"):
        index_hdfs_dir_path = entry.cfg_mgr.hdfs.get_tmp_path("not_exists_section",
                                                              "not_exists_option",
                                                              "index")
    else:
        index_hdfs_dir_path = entry.cfg_mgr.hdfs.get_result_path("not_exists_section",
                                                                 "not_exists_option",
                                                                 "index")
    return os.path.join(index_hdfs_dir_path, name)


def check_index_hdfs_data_exists(entry: Entry, name: str, logger=None):
    index_hdfs_path = get_index_hdfs_path(entry, name)
    out = Shell(logger).build(f"hadoop fs -du -h {index_hdfs_path}").execute().stdout

    if "No such file or directory" in out:
        return False
    elif re.search(rf"{index_hdfs_path}", out):
        return False


def get_project_index_result_path(entry: Entry):
    return os.path.join(entry.path_mgr.get_project_root_path(entry.proc, entry.settings),
                        "tmp",
                        f"{entry.path_mgr.get_module_py_path(entry.proc)}.json")


def register(*, name: str, desc: str = None,
             dependencies: typing.List[str] = None,
             persistent: str = None, persistent_param: typing.Dict = None,
             cache: bool = False, cache_level: StorageLevel = None):
    def wrap(df_func: typing.Callable[..., DataFrame]):
        def inner(entry: Entry, param: Parameter, logger: logging.Logger, *args, **kwargs):
            pass

        Executor.register(name=name, df_func=df_func, desc=desc,
                          dependencies=dependencies,
                          persistent=persistent, persistent_param=persistent_param,
                          cache=cache, cache_level=cache_level)
        return inner

    return wrap


class Executor:
    _views: typing.Dict[str, View] = {}

    @staticmethod
    def df(index):
        view = Executor._views.get(index)
        if view:
            return view.rst_df

    @staticmethod
    def register(*, name: str,
                 df_func: typing.Union[typing.Callable[..., DataFrame], DataFrame],
                 desc: str = None,
                 dependencies: typing.List[str] = None,
                 persistent: str = None, persistent_param: typing.Dict = None,
                 cache: bool = False, cache_level: StorageLevel = None):
        Executor._views[name] = View(name, desc,
                                     persistent, persistent_param,
                                     cache, cache_level,
                                     dependencies,
                                     df_func)

    @classmethod
    def execute(cls, entry: Entry, param: Parameter, logger, *indices):
        check_cycle_dependencies(cls._views)
        ret_mgr = ResultManager(get_project_index_result_path(entry), True, logger)
        for idx in indices:
            logger.info(f"target {idx} execute starting")
            try:
                Executor._execute(idx, entry, param, ret_mgr, logger)
            except:
                logger.error(f"target {idx} execute failed")
                logger.error(traceback.format_exc())
            else:
                logger.info(f"target {idx} execute finished")
        return ret_mgr.json

    @classmethod
    def merge(cls, entry: Entry, target_df: DataFrame, ext_index_df: DataFrame, target_columns: typing.List[str], indices: typing.List[str],
              join_columns: typing.List[str], hdfs_tmp_path, slice_step_num=5, partition_num=None):
        index_df_list = [cls.df(idx) for idx in indices]
        index_df_list.append(ext_index_df)
        indices = list(set(ext_index_df.columns).difference(join_columns)) + indices
        return index_join_merge(entry.spark, target_df, target_columns, index_df_list, indices,
                                join_columns, hdfs_tmp_path, slice_step_num, partition_num)

    @classmethod
    def _execute(cls, index, entry: Entry, param: Parameter, ret_mgr, logger):
        logger.info(f"{index} execute starting")
        view = Executor._views.get(index)
        dependencies = view.dependencies
        logger.info(f"{index} depends on index view {dependencies}")
        for depend in dependencies:
            Executor._execute(depend, entry, param, ret_mgr, logger)
        try:
            view.execute(entry, param, ret_mgr, logger)
        except:
            logger.error(f"{index} execute failed")
            raise
        else:
            logger.info(f"{index} execute finished")
