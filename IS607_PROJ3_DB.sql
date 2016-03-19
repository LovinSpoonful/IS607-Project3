
-- Create database evaluating relative desirability of data science skills
DROP SCHEMA IF EXISTS skill;
CREATE SCHEMA skill;
USE skill;

-- Create the tbl_import
-- initial staging area for raw CSV's
DROP TABLE IF EXISTS `tbl_import`;
CREATE TABLE `tbl_import` (
`source_id`           int(11)   NOT NULL,
`skill_type_name`     char(100) NOT NULL,
`skill_name`          char(100) NOT NULL,
`rating`              float     NOT NULL);

-- distinct skills from tbl_import
DROP TABLE IF EXISTS `tbl_skill`;
CREATE TABLE `tbl_skill` (
`skill_id`           int(11)   NOT NULL auto_increment,
`skill_type_id`      int(11)   NOT NULL,
`skill_name`         char(100) NOT NULL,
`skill_description`  char(255) NULL,
PRIMARY KEY (`skill_id`));

-- distinct skill types from tbl_import
DROP TABLE IF EXISTS `tbl_skill_type`;
CREATE TABLE `tbl_skill_type` (
`skill_type_id`           int(11)   NOT NULL,
`skill_type_name`         char(100) NOT NULL,
`skill_type_description`  char(255) NULL,
PRIMARY KEY (`skill_type_id`));

-- distinct sources from tbl_import
DROP TABLE IF EXISTS `tbl_source`;
CREATE TABLE `tbl_source` (
`source_id`           int(11)   NOT NULL,
`source_name`         char(100) NOT NULL,
`source_description`  char(255) NULL,
`source_URL`          char(255) NULL,
PRIMARY KEY (`source_id`));

ALTER TABLE tbl_skill
ADD FOREIGN KEY fk_skill_type(skill_type_id)
REFERENCES tbl_skill_type(skill_type_id)
ON DELETE NO ACTION
ON UPDATE CASCADE;

-- normalized representation of imported data
DROP TABLE IF EXISTS `tbl_data_n`;
CREATE TABLE `tbl_data_n` (
`skill_type_id`       int(11) NOT NULL,
`skill_id`            int(11) NOT NULL,
`source_id`           int(11) NOT NULL,
`rating`              float   NOT NULL,
PRIMARY KEY (`skill_type_id`, `skill_id`, `source_id`));

ALTER TABLE tbl_data_n
ADD FOREIGN KEY fk_skill_type(skill_type_id)
REFERENCES tbl_skill_type(skill_type_id)
ON DELETE NO ACTION
ON UPDATE CASCADE;

ALTER TABLE tbl_data_n
ADD FOREIGN KEY fk_skill(skill_id)
REFERENCES tbl_skill(skill_id)
ON DELETE NO ACTION
ON UPDATE CASCADE;


-- denormalized representation of data
-- to ease view generation
DROP TABLE IF EXISTS `tbl_data`;
CREATE TABLE `tbl_data` (
`skill_type_id`       int(11)   NULL,
`skill_id`            int(11)   NULL,
`source_id`           int(11)   NULL,
`skill_type_name`     char(100) NULL,
`skill_name`          char(100) NULL,
`source_name`         char(100) NULL,
`source_description`  char(100) NULL,
`rating`              float     NULL,
`z_score_global`      float     NULL,
`rel_rank_global`     float     NULL,
`scalar_global`       float     NULL,
`z_score_local`       float     NULL,
`rel_rank_local`      float     NULL,
`scalar_local`        float     NULL)








-- in case we need this
/*
LOAD DATA LOCAL INFILE 'C:/Data/tb.csv'
INTO TABLE tb
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
(country, year, sex, @child, @adult, @elderly)
SET
child = nullif(@child,-1),
adult = nullif(@adult,-1),
elderly = nullif(@elderly,-1)
;
*/




