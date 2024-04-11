DROP TABLE IF EXISTS `user`;

CREATE TABLE `user`
(
    id BIGINT NOT NULL COMMENT '主键ID',
    name VARCHAR(30) NULL DEFAULT NULL COMMENT '姓名',
    age INT NULL DEFAULT NULL COMMENT '年龄',
    email VARCHAR(50) NULL DEFAULT NULL COMMENT '邮箱',
    org_ids VARCHAR(50) NULL DEFAULT NULL COMMENT '机构ids',
    update_time TIMESTAMP NULL DEFAULT NULL COMMENT '更新时间',
    create_by VARCHAR(50) NULL DEFAULT NULL COMMENT '创建人',
    version INT NULL DEFAULT 0 COMMENT '版本',
    amount DOUBLE NULL DEFAULT 0.00 COMMENT '存款',
    -- 'interval' VARCHAR(50) NULL DEFAULT NULL COMMENT '临时字段，测试keepGlobalFormat	',
    sex VARCHAR(10) NULL DEFAULT NULL COMMENT '性别 1-男，2-女',
    del_f VARCHAR(10) NULL DEFAULT NULL COMMENT '删除标志 1-删除 0-正常',
    PRIMARY KEY (id)
);