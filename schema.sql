drop table if exists distcp.distcp_detail2;
create table distcp.distcp_detail2 (id MEDIUMINT NOT NULL AUTO_INCREMENT,source VARCHAR(400) ,destination VARCHAR(400) ,PRIMARY KEY (id));
