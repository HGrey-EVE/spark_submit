"""
@author liuhao

Email: liuhao@bbdservice.com

Instruction: 二期出口骗税和转口贸易测试

Date: 2021/7/6 9:12
"""
import logging
import sys
import traceback
from pyspark import SparkConf
from pyspark.sql import SparkSession

from whetstone.common.settings import Setting
from whetstone.core.config_mgr import ConfigManager
from whetstone.core.entry import Entry
from whetstone.core.index import Executor, Parameter
from whetstone.core.path_mgr import PathManager
from whetstone.core.proc import ProcInfo


class CreateDF:

    @staticmethod
    def pboc_exp_custom_rpt_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', '101', '2021-06-01', '1001', 1000],
                                                 ['1000', '101', '2021-05-01', '1001', 2000],
                                                 ['1000', '102', '2021-06-01', '1210', 3000]],
                                           schema=['corp_code', 'trade_country_code', 'exp_date',
                                                   'custom_trade_mode_code', 'deal_amt_usd']
                                           )

    @staticmethod
    def pboc_corp_rcv_jn_v(entry: Entry):
        return entry.spark.createDataFrame(
            data=[['1000', 1000, '2021-06-01', 2000, '121010', 2000, 'jack', 'T', '1001', '1012', '123', '123'],
                  ['1000', 5000, '2021-05-01', 3000, '121010', 5000, 'rose', 'T', '1212', '1212', '256', '333'],
                  ['1000', 8000, '2021-06-01', 10000, '121010', 10000, 'james', 'T', '1201', '1305', '269', '343']],
            schema=['corp_code', 'deal_amt_usd', 'rcv_date', 'salefx_amt_usd',
                    'tx_code', 'tx_amt_usd', 'cp_payer_name', 'settle_method_code',
                    'safecode', 'bank_code', 'rptno', 'payer_country_code'])

    @staticmethod
    def pboc_corp_pay_jn_v(entry: Entry):
        return entry.spark.createDataFrame(
            data=[['1000', '101', '2021-06-01', 1000, '121010', '1001', '1012', '134', 'T'],
                  ['1000', '101', '2021-05-01', 2000, '121010', '1212', '1212', '625', 'T'],
                  ['1000', '102', '2021-06-01', 50000, '121010', '1201', '1305', '745', 'T']],
            schema=['corp_code', 'payer_country_code', 'pay_date', 'tx_amt_usd', 'tx_code',
                    'safecode', 'bank_code', 'rptno', 'settle_method_code'])

    @staticmethod
    def target_company_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', 'bbd有限公司'],
                                                 ['1001', '东信北邮有限公司'],
                                                 ['1002', 'www有限公司']],
                                           schema=['corp_code', 'company_name'])

    @staticmethod
    def all_off_line_relations_degree2_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', 'bbd有限公司', 2, 2, '法人', '王二', '张三', '1', '1'],
                                                 ['1001', '东信北邮有限公司', 2, 2, '法人', '李武', '李璐', '1', '1'],
                                                 ['1002', 'www有限公司', 2, 2, '法人', '刘二', '刘三', '1', '1']],
                                           schema=['corp_code', 'company_rel_degree_2', 'source_degree',
                                                   'destination_degree', 'position', 'source_name', 'destination_name',
                                                   'source_isperson', 'destination_isperson'])

    @staticmethod
    def all_company_v(entry: Entry):
        return entry.spark.createDataFramedata(data=[['1000', 'bbd有限公司'],
                                                     ['1001', '东信北邮有限公司'],
                                                     ['1002', 'www有限公司']],
                                               schema=['corp_code', 'company_name'])

    # @staticmethod
    # def pboc_expf_v(entry: Entry):
    #     return entry.spark.createDataFrame(data=[['1000', '101', '2021-06-01', 1000],
    #                                              ['1000', '101', '2021-05-01', 2000],
    #                                              ['1000', '102', '2021-06-01', 50000]],
    #                                        schema=['corp_code', 'payer_country_code', 'imp_date', 'deal_amt_usd'])

    @staticmethod
    def pboc_expf_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', '2021-06-01', '201', 10],
                                                 ['1000', '2021-05-01', '201', 30],
                                                 ['1000', '2021-06-01', '202', 1000]],
                                           schema=['corp_code', 'exp_date', 'merch_code', 'prod_deal_amt_usd'])

    @staticmethod
    def commodity_tax_rebate_rate_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', '2021-06-01', '201', 10, '2021-06-01'],
                                                 ['1000', '2021-05-01', '201', 30, '2021-06-01'],
                                                 ['1000', '2021-06-01', '202', 1000, '2021-06-01']],
                                           schema=['corp_code', 'start_date', 'goods_code', 'tax_rebate_rate',
                                                   'end_date'])

    @staticmethod
    def qyxx_basic_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', '101', '2021-06-01', 1000, 'bbd有限公司'],
                                                 ['1000', '101', '2021-05-01', 2000, '东信北邮有限公司'],
                                                 ['1000', '102', '2021-06-01', 50000, 'www有限公司']],
                                           schema=['bbd_qyxx_id', 'company_type', 'imp_date', 'deal_amt_usd',
                                                   'company_name'])

    @staticmethod
    def company_type_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', '101', '2021-06-01', '国有'],
                                                 ['1000', '101', '2021-05-01', '非国有'],
                                                 ['1000', '102', '2021-06-01', '国资']],
                                           schema=['bbd_qyxx_id', 'code_value', 'imp_date', 'code_desc'])

    @staticmethod
    def pboc_corp_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', '王二', '2021-06-01', '张三'],
                                                 ['1000', '刘二', '2021-05-01', '李璐'],
                                                 ['1000', '刘三', '2021-06-01', '黄新']],
                                           schema=['corp_code', 'contact_name', 'deal_date', 'frname'])

    @staticmethod
    def t_acct_rpb_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', 500, '2021-06-01', 300],
                                                 ['1000', 800, '2021-05-01', 400],
                                                 ['1000', 1000, '2021-06-01', 2000]],
                                           schema=['corp_code', 'credit_amt_usd', 'deal_date', 'debit_amt_usd'])

    @staticmethod
    def pboc_imp_custom_rpt_v(entry: Entry):
        return entry.spark.createDataFrame(data=[['1000', '101', '2021-06-01', '1001', 1000],
                                                 ['1000', '101', '2021-05-01', '1001', 2000],
                                                 ['1000', '102', '2021-06-01', '1210', 3000]],
                                           schema=['corp_code', 'trade_country_code', 'imp_date',
                                                   'custom_trade_mode_code', 'deal_amt_usd']
                                           )


'''
进行指标测试，
但只验证指标的sql正确性，
不对结果逻辑进行验证
'''


class TestIndex:

    def setup_class(self):
        _proc = ProcInfo(project="scwgj", module="index_compute", env="dev", version="20210501", debug=True)
        _settings = Setting()
        _path_mgr = PathManager()
        _cfg_mgr = ConfigManager(_proc, _settings)

        _logger = logging.getLogger()
        _logger.addHandler(logging.StreamHandler(sys.stdout))
        _logger.setLevel(logging.INFO)

        conf = SparkConf()
        conf.setAppName("test").setMaster("local[1]")
        session = SparkSession \
            .builder \
            .appName(f"test") \
            .config(conf=conf).getOrCreate()

        self.entry = Entry() \
            .build_proc(_proc) \
            .build_spark(session) \
            .build_cfg_mgr(_cfg_mgr) \
            .build_logger(_logger) \
            .build_path_mgr(_path_mgr) \
            .build_settings(_settings)

        target_company_df = self.entry.spark.read.csv("./new_company_list-20200714.csv", header=True, sep=",")
        Executor.register(name=f"target_company_v", df_func=target_company_df, dependencies=[])

    def teardown_class(self):
        self.entry.spark.stop()

    # 出口骗税--出口抵运国数量与收汇交易对手国别数量的比例
    def test_export_ctr_ct_ratio(self):
        from scwgj.index_compute.indices.export_ctr_ct_ratio import export_ctr_ct_ratio
        assert export_ctr_ct_ratio

        pboc_exp_custom_rpt_v = CreateDF.pboc_exp_custom_rpt_v(entry=self.entry)
        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        Executor.register(name=f"pboc_exp_custom_rpt_v", df_func=pboc_exp_custom_rpt_v, dependencies=[])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05",
                "pboc_exp_custom_rpt": "2021-07-05"
            }
        })
        index_name = 'export_ctr_ct_ratio'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from export_ctr_ct_ratio """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税--出口金额与境内关联方进口金额比例
    def test_export_rel_import_ratio(self):
        from scwgj.index_compute.indices.export_rel_import_ratio import export_rel_import_ratio
        assert export_rel_import_ratio

        pboc_exp_custom_rpt_v = CreateDF.pboc_exp_custom_rpt_v(entry=self.entry)
        all_off_line_relations_degree2_v = CreateDF.all_off_line_relations_degree2_v(entry=self.entry)
        all_company_v = CreateDF.all_company_v(entry=self.entry)
        pboc_imp_custom_rpt_v = CreateDF.pboc_imp_custom_rpt_v(entry=self.entry)
        Executor.register(name=f"pboc_exp_custom_rpt_v", df_func=pboc_exp_custom_rpt_v, dependencies=[])
        Executor.register(name=f"all_off_line_relations_degree2_v", df_func=all_off_line_relations_degree2_v,
                          dependencies=[])
        Executor.register(name=f"all_company_v", df_func=all_company_v, dependencies=[])
        Executor.register(name=f"pboc_imp_custom_rpt_v", df_func=pboc_imp_custom_rpt_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_imp_custom_rpt": "2021-07-05",
                "pboc_exp_custom_rpt": "2021-07-05"
            }
        })
        index_name = 'export_rel_import_ratio'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from export_rel_import_ratio """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期新增	出口商品计数
    def test_export_goods_ct(self):
        from scwgj.index_compute.indices.export_goods_ct import export_goods_ct
        assert export_goods_ct

        pboc_expf_v = CreateDF.pboc_expf_v(entry=self.entry)
        Executor.register(name=f"pboc_expf_v", df_func=pboc_expf_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_expf": "2021-07-05"
            }
        })
        index_name = 'export_goods_ct'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from export_goods_ct """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期新增	出口商品退税率
    def test_export_goods_taxrate(self):
        from scwgj.index_compute.indices.export_goods_taxrate import export_goods_taxrate
        assert export_goods_taxrate

        commodity_tax_rebate_rate_v = CreateDF.commodity_tax_rebate_rate_v(entry=self.entry)
        pboc_expf_v = CreateDF.pboc_expf_v(entry=self.entry)
        Executor.register(name=f"commodity_tax_rebate_rate_v", df_func=commodity_tax_rebate_rate_v, dependencies=[])
        Executor.register(name=f"pboc_expf_v", df_func=pboc_expf_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "commodity_tax_rebate_rate": "2021-07-05",
                "pboc_expf_v": "2021-07-05"
            }
        })
        index_name = 'export_goods_taxrate'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from export_goods_taxrate """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期新增	出口收汇金额（一般贸易）
    def test_corp_inc_amt(self):
        from scwgj.index_compute.indices.corp_inc_amt import corp_inc_amt
        assert corp_inc_amt

        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05"
            }
        })
        index_name = 'corp_inc_amt'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from corp_inc_amt """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期新增	出口收汇金额变异系数（按月）
    def test_corp_inc_amt_varicoef(self):
        from scwgj.index_compute.indices.corp_inc_amt_varicoef import corp_inc_amt_varicoef
        assert corp_inc_amt_varicoef

        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05"
            }
        })
        index_name = 'corp_inc_amt_varicoef'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from corp_inc_amt_varicoef """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期新增	经济类型（内部数据）
    def test_business_type(self):
        from scwgj.index_compute.indices.business_type import business_type
        assert business_type

        qyxx_basic_v = CreateDF.qyxx_basic_v(entry=self.entry)
        all_off_line_relations_degree2_v = CreateDF.all_off_line_relations_degree2_v(entry=self.entry)
        all_company_v = CreateDF.all_company_v(entry=self.entry)
        Executor.register(name=f"qyxx_basic_v", df_func=qyxx_basic_v, dependencies=[])
        Executor.register(name=f"all_off_line_relations_degree2_v", df_func=all_off_line_relations_degree2_v,
                          dependencies=[])
        Executor.register(name=f"all_company_v", df_func=all_company_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
            }
        })
        index_name = 'business_type'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from business_type """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期更新	经营是否异常（包含被行政执法部门处罚）
    def test_black_abnormal(self):
        from scwgj.index_compute.indices.black_abnormal import black_abnormal
        assert black_abnormal

        legal_adjudicative_documents_v = CreateDF.legal_adjudicative_documents_v(entry=self.entry)
        risk_punishment_v = CreateDF.risk_punishment_v(entry=self.entry)
        legal_persons_subject_to_enforcement_v = CreateDF.legal_persons_subject_to_enforcement_v(entry=self.entry)
        legal_dishonest_persons_subject_to_enforcement_v = CreateDF.legal_dishonest_persons_subject_to_enforcement_v(
            entry=self.entry)
        risk_tax_owed_v = CreateDF.risk_tax_owed_v(entry=self.entry)
        Executor.register(name=f"legal_adjudicative_documents_v", df_func=legal_adjudicative_documents_v,
                          dependencies=[])
        Executor.register(name=f"risk_punishment_v", df_func=risk_punishment_v, dependencies=[])
        Executor.register(name=f"legal_persons_subject_to_enforcement_v",
                          df_func=legal_persons_subject_to_enforcement_v, dependencies=[])
        Executor.register(name=f"legal_dishonest_persons_subject_to_enforcement_v",
                          df_func=legal_dishonest_persons_subject_to_enforcement_v, dependencies=[])
        Executor.register(name=f"risk_tax_owed_v", df_func=risk_tax_owed_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
            }
        })
        index_name = 'black_abnormal'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from black_abnormal """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期新增	境内多个企业仅与同一境外交易对手发生货物贸易收入
    def test_same_foreign_partner(self):
        from scwgj.index_compute.indices.same_foreign_partner import same_foreign_partner
        assert same_foreign_partner

        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05"
            }
        })
        index_name = 'same_foreign_partne'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from same_foreign_partne """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 出口骗税	二期新增	收汇当日或次日账户贷借相等天数占比
    def test_equal_in_out_ratio(self):
        from scwgj.index_compute.indices.equal_in_out_ratio import equal_in_out_ratio
        assert equal_in_out_ratio

        t_acct_rpb_v = CreateDF.t_acct_rpb_v(entry=self.entry)
        Executor.register(name=f"t_acct_rpb_v", df_func=t_acct_rpb_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05"
            }
        })
        index_name = 'equal_in_out_ratio'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from equal_in_out_ratio """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    'pboc_corp_rcv_jn_v', 'target_company_v', 'pboc_corp_pay_jn_v',
    'all_off_line_relations_degree2_v', 'all_company_v'

    # 出口骗税	二期新增	"一般贸易收汇（企业本身）与境内关联方（一二度关联、国外合作伙伴、）付汇金额比例
    def test_collect_relpay_ratio(self):
        from scwgj.index_compute.indices.collect_relpay_ratio import collect_relpay_ratio
        assert collect_relpay_ratio

        all_off_line_relations_degree2_v = CreateDF.all_off_line_relations_degree2_v(entry=self.entry)
        all_company_v = CreateDF.all_company_v(entry=self.entry)
        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        pboc_corp_pay_jn_v = CreateDF.pboc_corp_pay_jn_v(entry=self.entry)
        Executor.register(name=f"all_off_line_relations_degree2_v", df_func=all_off_line_relations_degree2_v,
                          dependencies=[])
        Executor.register(name=f"all_company_v", df_func=all_company_v, dependencies=[])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        Executor.register(name=f"pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05",
                "pboc_corp_pay_jn": "2021-07-05"
            }
        })
        index_name = 'collect_relpay_ratio'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from collect_relpay_ratio """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 转口贸易	二期新增	关联企业只做转口贸易
    def test_rel_only_entreport(self):
        from scwgj.index_compute.indices.rel_only_entreport import rel_only_entreport
        assert rel_only_entreport

        all_off_line_relations_degree2_v = CreateDF.all_off_line_relations_degree2_v(entry=self.entry)
        all_company_v = CreateDF.all_company_v(entry=self.entry)
        pboc_corp_v = CreateDF.pboc_corp_v(entry=self.entry)
        pboc_corp_pay_jn_v = CreateDF.pboc_corp_pay_jn_v(entry=self.entry)
        Executor.register(name=f"all_off_line_relations_degree2_v", df_func=all_off_line_relations_degree2_v,
                          dependencies=[])
        Executor.register(name=f"all_company_v", df_func=all_company_v, dependencies=[])
        Executor.register(name=f"pboc_corp_v", df_func=pboc_corp_v, dependencies=[])
        Executor.register(name=f"pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp": "2021-07-05",
                "pboc_corp_pay_jn": "2021-07-05"
            }
        })
        index_name = 'rel_only_entreport'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from rel_only_entreport """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 转口贸易	二期新增	异地办理转口贸易收支比例
    def test_entreport_elsewhere(self):
        from scwgj.index_compute.indices.entreport_elsewhere import entreport_elsewhere
        assert entreport_elsewhere

        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        pboc_corp_pay_jn_v = CreateDF.pboc_corp_pay_jn_v(entry=self.entry)
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        Executor.register(name=f"pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05",
                "pboc_corp_pay_jn": "2021-07-05"
            }
        })
        index_name = 'entreport_elsewhere'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from entreport_elsewhere """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 转口贸易	二期更新	转口贸易付汇额与贸易融资的比例
    def test_corp_signamt_txamt_entreport_prop(self):
        from scwgj.index_compute.indices.corp_signamt_txamt_entreport_prop import corp_signamt_txamt_entreport_prop
        assert corp_signamt_txamt_entreport_prop

        pboc_dfxloan_sign_v = CreateDF.pboc_dfxloan_sign_v(entry=self.entry)
        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        pboc_corp_pay_jn_v = CreateDF.pboc_corp_pay_jn_v(entry=self.entry)
        Executor.register(name=f"pboc_dfxloan_sign_v", df_func=pboc_dfxloan_sign_v, dependencies=[])
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        Executor.register(name=f"pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05",
                "pboc_corp_pay_jn": "2021-07-05",
                "pboc_dfxloan_sign": "2021-07-05"
            }
        })
        index_name = 'corp_signamt_txamt_entreport_prop'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from corp_signamt_txamt_entreport_prop """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')

    # 转口贸易	二期新增	转口贸易收支电汇比例
    def test_entreport_wire_ratio(self):
        from scwgj.index_compute.indices.entreport_wire_ratio import entreport_wire_ratio
        assert entreport_wire_ratio

        pboc_corp_rcv_jn_v = CreateDF.pboc_corp_rcv_jn_v(entry=self.entry)
        pboc_corp_pay_jn_v = CreateDF.pboc_corp_pay_jn_v(entry=self.entry)
        Executor.register(name=f"pboc_corp_rcv_jn_v", df_func=pboc_corp_rcv_jn_v, dependencies=[])
        Executor.register(name=f"pboc_corp_pay_jn_v", df_func=pboc_corp_pay_jn_v, dependencies=[])
        param = Parameter(**{
            "tmp_dict_dict": {
                "pboc_corp_rcv_jn": "2021-07-05",
                "pboc_corp_pay_jn": "2021-07-05"
            }
        })
        index_name = 'entreport_wire_ratio'
        Executor.execute(self.entry, param, self.entry.logger, index_name)
        try:
            result_df = self.entry.spark.sql("""select * from entreport_wire_ratio """)
            self.entry.logger.info(f'{index_name}的执行结果为：\n{result_df.show()}')
        except Exception:
            self.entry.logger.info(f'{index_name}报错，错误信息为：\n{traceback.format_exc()}')
