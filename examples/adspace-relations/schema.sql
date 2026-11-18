CREATE TABLE demo_ad_spaces (
    id BIGINT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    enabled TINYINT(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE demo_ad_creatives (
    id BIGINT PRIMARY KEY,
    ad_space_id BIGINT NOT NULL,
    title VARCHAR(128) NOT NULL,
    FOREIGN KEY (ad_space_id) REFERENCES demo_ad_spaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE demo_ad_rules (
    id BIGINT PRIMARY KEY,
    ad_space_id BIGINT NOT NULL,
    region VARCHAR(64) NOT NULL,
    FOREIGN KEY (ad_space_id) REFERENCES demo_ad_spaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

START TRANSACTION;

INSERT INTO demo_ad_spaces (id, name, enabled) VALUES
    (6111, '首页广告位', 1),
    (6112, '详情页广告位', 1);

INSERT INTO demo_ad_creatives (id, ad_space_id, title) VALUES
    (101, 6111, '春季促销'),
    (102, 6111, '会员活动'),
    (103, 6112, '详情页横幅');

INSERT INTO demo_ad_rules (id, ad_space_id, region) VALUES
    (201, 6111, '上海'),
    (202, 6111, '北京'),
    (203, 6111, '广州');

COMMIT;
