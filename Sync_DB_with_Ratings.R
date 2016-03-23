
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

# weighted_rating_by_skill_type

dbWriteTable(skilldb, name="df_temp1", value=data.frame.name)


dbSendQuery(con, "
            UPDATE TBL_DATA T, DF_TEMP1 R 
               SET T.WEIGHTED_RATING_BY_SKILL_TYPE = R.WEIGHTED_RATING_BY_SKILL_TYPE 
             WHERE T.SKILL_ID = R.SKILL_ID 
               AND T.SOURCE_ID = R.SOURCE_ID;")



# overall_weighted_rating

dbWriteTable(skilldb, name="df_temp2", value=data.frame.name)


dbSendQuery(con, "
            UPDATE TBL_DATA T, DF_TEMP2 R 
               SET T.OVERALL_WEIGHTED_RATING = R.OVERALL_WEIGHTED_RATING 
             WHERE T.SKILL_ID = R.SKILL_ID AND T.SOURCE_ID = R.SOURCE_ID;")



# rating_by_skill_set

dbWriteTable(skilldb, name="df_temp3", value=data.frame.name)


dbSendQuery(con, "
            UPDATE TBL_DATA T, DF_TEMP2 R 
               SET T.RATING_BY_SKILL_SET = R.RATING_BY_SKILL_SET  
             WHERE T.SKILL_ID = R.SKILL_ID AND T.SOURCE_ID = R.SOURCE_ID;")

dbSendQuery(mydb, "DROP TABLE IF EXISTS DF_TEMP1, DF_TEMP2, DF_TEMP3")



