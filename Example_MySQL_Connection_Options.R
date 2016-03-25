
library(RMySQL)


# MySQL DB info
proj_user <- "project3"
proj_pwd  <- "CUNYRBridge4"
proj_db   <- "skill"
proj_host <- "db4free.net"

## ------------------------------------------
## Using RMYSQL
## ------------------------------------------

# establish the connection to the skill DB on db4free.net
skilldb = dbConnect(MySQL(), user=proj_user, password=proj_pwd, dbname=proj_db, host=proj_host)

# load the processed source data into a dataframe from the tbl_data
rs <- dbGetQuery(skilldb, "select * from tbl_data")


# load a view into a dataframe
my_view <- dbGetQuery(skilldb, "select * from vw_top5_skill_sets_by_skill_type")
my_view  #get a look at it

#close the connection
dbDisconnect(skilldb)

## ------------------------------------------
## Alternative option using dplyr
## ------------------------------------------

library(dplyr)

conDplyr = src_mysql(dbname = proj_db, user = proj_user, password = proj_pwd, host = proj_host)

skillData <- conDplyr %>%
  tbl("tbl_data") %>%
  collect() 

# load some data from the tbl_import table
importData <- conDplyr %>%
  tbl("tbl_import") %>%
  collect()  %>% filter(row_number() < 100)



