
-- Create a database to help evaluate relative desirability of data science skills
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

-- normalized representation of imported data
DROP TABLE IF EXISTS `tbl_data_n`;
CREATE TABLE `tbl_data_n` (
`skill_type_id`       int(11) NOT NULL,
`skill_set_id`        int(11)   NOT NULL,
`skill_id`            int(11) NOT NULL,
`source_id`           int(11) NOT NULL,
`rating`              float   NOT NULL,
PRIMARY KEY (`skill_type_id`, `skill_id`, `source_id`));

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




CREATE TABLE `tbl_skill_set_xref` (
  `skill_set_id` int(11) NOT NULL,
  `skill_id` int(11) NOT NULL,
  PRIMARY KEY (`skill_set_id`,`skill_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- static table
DROP TABLE IF EXISTS `tbl_skill_set`;
CREATE TABLE `tbl_skill_set` (
`skill_set_id`           int(11)   NOT NULL auto_increment,
`skill_set_name`         char(100) NOT NULL,
`skill_set_description`  char(255) NULL,
PRIMARY KEY (`skill_set_id`));


-- static table
DROP TABLE IF EXISTS `tbl_skill_category_sets`;
CREATE TABLE `tbl_skill_category_sets` (
`skill_set_name`         char(100) NOT NULL,
`skill_set_description`  char(255) NULL,
PRIMARY KEY (`skill_set_name`));


DROP TABLE IF EXISTS `tbl_skills_categories`;
CREATE TABLE `tbl_skills_categories` (
`skill_set_name`       int(11)   NOT NULL,
`skill_name`           int(11)   NOT NULL, 
PRIMARY KEY (`skill_set_name`,`skill_name`));


-- distinct sources from tbl_import
DROP TABLE IF EXISTS `tbl_source`;
CREATE TABLE `tbl_source` (
`source_id`           int(11)   NOT NULL,
`source_name`         char(100) NOT NULL,
`source_description`  char(255) NULL,
`source_URL`          char(255) NULL,
PRIMARY KEY (`source_id`));

-- removed foreighn keys so r code could repeat
/*
ALTER TABLE tbl_skill
ADD FOREIGN KEY fk_skill_type(skill_type_id)
REFERENCES tbl_skill_type(skill_type_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE tbl_data_n
ADD FOREIGN KEY fk_skill_type(skill_type_id)
REFERENCES tbl_skill_type(skill_type_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE tbl_data_n
ADD FOREIGN KEY fk_skill(skill_id)
REFERENCES tbl_skill(skill_id)
ON DELETE NO ACTION
ON UPDATE NO ACTION;
*/


-- denormalized representation of data
-- to make view generation easier
DROP TABLE IF EXISTS `tbl_data`;
CREATE TABLE `tbl_data` (
`skill_type_id`       int(11)   NULL,
`skill_set_id`        int(11)   NOT NULL,
`skill_id`            int(11)   NULL,
`source_id`           int(11)   NULL,
`skill_type_name`     char(100) NULL,
`skill_set_name`      char(100) NOT NULL,
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




