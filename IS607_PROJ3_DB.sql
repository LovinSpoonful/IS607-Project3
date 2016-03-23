
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

-- distinct sources from tbl_import
DROP TABLE IF EXISTS `tbl_source`;
CREATE TABLE `tbl_source` (
`source_id`           int(11)   NOT NULL,
`source_name`         char(100) NOT NULL,
`source_description`  char(255) NULL,
`source_URL`          char(255) NULL,
PRIMARY KEY (`source_id`));

-- distinct skills
DROP TABLE IF EXISTS `tbl_skill`;
CREATE TABLE `tbl_skill` (
`skill_id`           int(11)   NOT NULL auto_increment,
`skill_set_id`       int(11)   NULL,
`skill_type_id`      int(11)   NOT NULL,
`skill_name`         char(100) NOT NULL,
`skill_description`  char(255) NULL,
PRIMARY KEY (`skill_id`));

-- distinct skill types
DROP TABLE IF EXISTS `tbl_skill_type`;
CREATE TABLE `tbl_skill_type` (
`skill_type_id`           int(11)   NOT NULL,
`skill_type_name`         char(100) NOT NULL,
`skill_type_description`  char(255) NULL,
PRIMARY KEY (`skill_type_id`));

DROP TABLE IF EXISTS `tbl_skill_set`;
CREATE TABLE `tbl_skill_set` (
`skill_set_id`           int(11)   NOT NULL auto_increment,
`skill_type_id`          int(11)   NULL,
`skill_set_name`         char(100) NULL,
`skill_set_description`  char(255) NULL,
PRIMARY KEY (`skill_set_id`));

DROP TABLE IF EXISTS `tbl_skill_set_xref`;
CREATE TABLE `tbl_skill_set_xref` (  
 `skill_set_id` int(11) NOT NULL,  
 `skill_id` int(11) NOT NULL, 
 PRIMARY KEY (`skill_set_id`,`skill_id`));  

-- normalized representation
DROP TABLE IF EXISTS `tbl_data_n`;
CREATE TABLE `tbl_data_n` (
`skill_type_id`       int(11) NOT NULL,
`skill_set_id`        int(11)   NOT NULL,
`skill_id`            int(11) NOT NULL,
`source_id`           int(11) NOT NULL,
`rating`              float   NOT NULL,
PRIMARY KEY (`skill_type_id`, `skill_id`, `source_id`));

-- denormalized representation of data
-- to make view generation easier
DROP TABLE IF EXISTS `tbl_data`;
CREATE TABLE `tbl_data` (
`skill_type_id`           int(11)   NULL,
`skill_set_id`            int(11)   NOT NULL,
`skill_id`                int(11)   NULL,
`source_id`               int(11)   NULL,
`skill_type_name`         char(100) NULL,
`skill_set_name`          char(100) NOT NULL,
`skill_name`              char(100) NULL,
`source_name`             char(100) NULL,
`rating`                  float     NULL,
`rating_scalar`           float     NULL,
`weighted_rating_overall`         float     NULL,
`weighted_rating_by_skill_type`   float     NULL,
`weighted_rating_by_skill_set`    float     NULL)



