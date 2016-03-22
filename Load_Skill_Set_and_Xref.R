library(RMySQL)
library(dplyr)


# MySQL DB info
proj_user <- "project3"
proj_pwd  <- "CUNYRBridge4"
proj_db   <- "skill"
proj_host <- "db4free.net"

##################################################################
#
# Load tbl_skill_set from the CSV file on GitHub
#
##################################################################

# establish the connection to the skill DB on db4free.ne
skilldb = dbConnect(MySQL(), user=proj_user, password=proj_pwd, dbname=proj_db, host=proj_host)

# GitHub location 
URL <- "C:/Users/keith/Documents/DataScience/CUNY/DATA607/Projects/Project3/Skill_Categories.csv"

# read the skillsets file into a dataframe
skill_set <- read.csv(URL, header = TRUE, stringsAsFactors = FALSE)

# sync dataframe columns to match the target table

skill_set$skill_set_id <- rownames(skill_set)

colnames(skill_set) <- c("skill_set_name", "skill_set_description", "skill_type", "skill_set_id")


# set the column order in the dataframe to match the table structure
#skill_set <- skill_set[, c("skill_set_id", "skill_set_name", "skill_set_description")]

# pull down skill types from MySQL
skill_types <-  dbGetQuery(skilldb, "select skill_type_id, skill_type_name from tbl_skill_type")

skill_set_insert <-
                   skill_set %>%  
                   inner_join(skill_types, by=c("skill_type" = "skill_type_name")) %>%
                   select(skill_set_id, skill_type_id, skill_set_name, skill_set_description)


## delete the table tbl_skills_categories
del <- dbGetQuery(skilldb, "delete from tbl_skill_set")

# write the dataframe to the mySQL table
dbWriteTable(skilldb, value = skill_set_insert, name = "tbl_skill_set", append = TRUE, row.names = NA, header = FALSE) 

rs <- dbGetQuery(skilldb, "select count(*) from tbl_skill_set")

if (rs == 0) printf("No Rows Loaded into tbl_skill_set!")

#close the connection
dbDisconnect(skilldb)


##################################################################
#
# Load tbl_skill_set_xref from the CSV file on GitHub
#
##################################################################

# establish the connection to the skill DB on db4free.net
skilldb = dbConnect(MySQL(), user=proj_user, password=proj_pwd, dbname=proj_db, host=proj_host)

# GitHub location 
URL <- "https://raw.githubusercontent.com/LovinSpoonful/IS607-Project3/master/Skill_Category_Xref.csv"

# read the skills-to-skillset file into a dataframe
skill_category_xref <- read.csv(URL, header = TRUE, stringsAsFactors = FALSE)

# pull down the skill table for the skill_id and name
skills <-  dbGetQuery(skilldb, "select skill_id, skill_name from tbl_skill")

# pull down the skill set table for the sKill_set_id and name
skill_sets <- dbGetQuery(skilldb, "select skill_set_id, skill_set_name from tbl_skill_set")

# use dplr to build the dataframe to insert into tbl_skill_set_xref
skill_set_xref_insert <- 
                        skill_category_xref %>% 
                        inner_join(skills, by=c("Raw.Skill"="skill_name")) %>%
                        inner_join(skill_sets, by=c("Category"="skill_set_name"))  %>%
                        select(skill_set_id, skill_id)


## delete the table tbl_skills_categories
del <- dbGetQuery(skilldb, "delete from tbl_skill_set_xref")

# write the dataframe to the mySQL table
dbWriteTable(skilldb, value = skill_set_xref_insert, name = "tbl_skill_set_xref", append = TRUE, row.names = NA, header = FALSE) 

rs <- dbGetQuery(skilldb, "select count(*) from tbl_skill_set_xref")

if (rs == 0) printf("No Rows Loaded into tbl_skill_set_xref!")

#close the connection
dbDisconnect(skilldb)


