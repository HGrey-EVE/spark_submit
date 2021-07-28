use waiguanju;

CREATE TABLE `data_change` (
  `table_name` varchar(255) DEFAULT NULL COMMENT '原始数据对应数据导入的表',
  `uid` varchar(255) DEFAULT NULL COMMENT '原始数据的唯一ID',
  `properties` longtext COMMENT '要更新的属性json字符串: {"key1":"value1", "key2":"value2"}',
  `timestamp` datetime DEFAULT NULL COMMENT '数据更新时间',
  `operator` tinyint(4) DEFAULT NULL COMMENT '0表示更新，1表示删除,2表示新增'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `subject_test` (
  `bbd_qyxx_id` varchar(255) DEFAULT NULL COMMENT 'bbd_qyxx_id',
  `company_name` varchar(255) DEFAULT NULL COMMENT '公司名',
  `company_province` varchar(255) DEFAULT NULL COMMENT '所在省份',
  `company_county` varchar(255) DEFAULT NULL COMMENT '地区编码'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;