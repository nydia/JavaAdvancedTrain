DROP TABLE IF EXISTS `user`;

CREATE TABLE `user`
(
    id BIGINT NOT NULL COMMENT '主键ID',
    name VARCHAR(30) NULL DEFAULT NULL COMMENT '姓名',
    age INT NULL DEFAULT NULL COMMENT '年龄',
    email VARCHAR(50) NULL DEFAULT NULL COMMENT '邮箱',
    org_ids VARCHAR(50) NULL DEFAULT NULL COMMENT '机构ids',
    update_time TIMESTAMP NULL DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (id)
);