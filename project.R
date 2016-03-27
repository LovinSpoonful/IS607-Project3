library(RMySQL)
library(dplyr)

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

#close the connection
dbDisconnect(skilldb)

#rs <- read.csv("C:/Users/kfolsom/Desktop/New folder/Skills/tbl_data_kf.csv")

## ------------------------------------------
## Start the Ratings calculations
## ------------------------------------------
i <- 1
for (i in 1:NROW(rs))
{
  if(rs$source_name[i] == "Indeed")
  {
    rs$rating[i] <- rs$rating[i]/4
  }
  i <- i + 1
}
#get all google information
google <- subset(rs, rs$source_name == "Google")

#get all indeed information
indeed <- subset(rs, rs$source_name == "Indeed")

#get all kaggle information
kaggle <- subset(rs, rs$source_name == "Kaggle")

#Linkedin information
linkedin <- subset(rs, rs$source_name == "RJMETRICS")

#get all programming information
program <- subset(rs, rs$skill_type_name == "programming")

## WEIGHT SKILL TYPE BY SOURCE
#get the skill type name google
pgoogle <- subset(google, google$skill_type_name == "programming")
mgoogle <- subset(google, google$skill_type_name == "math")
bgoogle <- subset(google, google$skill_type_name == "business")
cgoogle <- subset(google, google$skill_type_name == "communication")
vgoogle <- subset(google, google$skill_type_name == "visualization")

totalgoogle <- NROW(google)

cpgoogle <- NROW(pgoogle)
cmgoogle <- NROW(mgoogle)
cbgoogle <- NROW(bgoogle)
ccgoogle <- NROW(cgoogle)
cvgoogle <- NROW(vgoogle)

pgoogle$weight_skill_type_source <- cpgoogle/totalgoogle
mgoogle$weight_skill_type_source <- cmgoogle/totalgoogle
bgoogle$weight_skill_type_source <- cbgoogle/totalgoogle
cgoogle$weight_skill_type_source <- ccgoogle/totalgoogle
vgoogle$weight_skill_type_source <- cvgoogle/totalgoogle

pgoogle$rating_skill_type_source <- pgoogle$rating * pgoogle$weight_skill_type_source
mgoogle$rating_skill_type_source <- mgoogle$rating * mgoogle$weight_skill_type_source
bgoogle$rating_skill_type_source <- bgoogle$rating * bgoogle$weight_skill_type_source
cgoogle$rating_skill_type_source <- cgoogle$rating * cgoogle$weight_skill_type_source
vgoogle$rating_skill_type_source <- vgoogle$rating * vgoogle$weight_skill_type_source

#get the skill type name indeed
pindeed <- subset(indeed, indeed$skill_type_name == "programming")
mindeed <- subset(indeed, indeed$skill_type_name == "math")
bindeed <- subset(indeed, indeed$skill_type_name == "business")
cindeed <- subset(indeed, indeed$skill_type_name == "communication")
vindeed <- subset(indeed, indeed$skill_type_name == "visualization")

totalindeed <- NROW(indeed)

cpindeed <- NROW(pindeed)
cmindeed <- NROW(mindeed)
cbindeed <- NROW(bindeed)
ccindeed <- NROW(cindeed)
cvindeed <- NROW(vindeed)

pindeed$weight_skill_type_source <- cpindeed/totalindeed
mindeed$weight_skill_type_source <- cmindeed/totalindeed
bindeed$weight_skill_type_source <- cbindeed/totalindeed
cindeed$weight_skill_type_source <- ccindeed/totalindeed
vindeed$weight_skill_type_source <- cvindeed/totalindeed

pindeed$rating_skill_type_source <- pindeed$rating * pindeed$weight_skill_type_source
mindeed$rating_skill_type_source <- mindeed$rating * mindeed$weight_skill_type_source
bindeed$rating_skill_type_source <- bindeed$rating * bindeed$weight_skill_type_source
cindeed$rating_skill_type_source <- cindeed$rating * cindeed$weight_skill_type_source
vindeed$rating_skill_type_source <- vindeed$rating * vindeed$weight_skill_type_source

#get the skill type name Kaggle
pkaggle <- subset(kaggle, kaggle$skill_type_name == "programming")
mkaggle <- subset(kaggle, kaggle$skill_type_name == "math")
bkaggle <- subset(kaggle, kaggle$skill_type_name == "business")
ckaggle <- subset(kaggle, kaggle$skill_type_name == "communication")
vkaggle <- subset(kaggle, kaggle$skill_type_name == "visualization")

totalkaggle <- NROW(kaggle)

cpkaggle <- NROW(pkaggle)
cmkaggle <- NROW(mkaggle)
cbkaggle <- NROW(bkaggle)
cckaggle <- NROW(ckaggle)
cvkaggle <- NROW(vkaggle)

pkaggle$weight_skill_type_source <- cpkaggle/totalkaggle
mkaggle$weight_skill_type_source <- cmkaggle/totalkaggle
bkaggle$weight_skill_type_source <- cbkaggle/totalkaggle
ckaggle$weight_skill_type_source <- cckaggle/totalkaggle
vkaggle$weight_skill_type_source <- cvkaggle/totalkaggle

pkaggle$rating_skill_type_source <- pkaggle$rating * pkaggle$weight_skill_type_source
mkaggle$rating_skill_type_source <- mkaggle$rating * mkaggle$weight_skill_type_source
bkaggle$rating_skill_type_source <- bkaggle$rating * bkaggle$weight_skill_type_source
ckaggle$rating_skill_type_source <- ckaggle$rating * ckaggle$weight_skill_type_source
vkaggle$rating_skill_type_source <- vkaggle$rating * vkaggle$weight_skill_type_source

dfrankskilltypesource <- rbind(mkaggle,pkaggle,ckaggle,bkaggle,vkaggle,pindeed, mindeed, bindeed, cindeed, vindeed, pgoogle, mgoogle, bgoogle, cgoogle, vgoogle)
################################################################################################################################################################

##WEIGHT SKILL TYPE OVERALL
#get all programming information
program <- subset(rs, rs$skill_type_name == "programming"&rs$source_name != "RJMETRICS")

#get all Math information
math <- subset(rs, rs$skill_type_name == "math"&rs$source_name != "RJMETRICS")

#get all business information
business <- subset(rs, rs$skill_type_name == "business"&rs$source_name != "RJMETRICS")

#get all communication information
comm <- subset(rs, rs$skill_type_name == "communication"&rs$source_name != "RJMETRICS")

#get all visualization information
visual <- subset(rs, rs$skill_type_name == "visualization"&rs$source_name != "RJMETRICS")

totalrs <- NROW(rs)

cpro <- NROW(program)
cmath <- NROW(math)
cbusiness <- NROW(business)
ccomm <- NROW(comm)
cvisual <- NROW(visual)

program$weight_skill_type_name_overall <- cpro/totalrs
math$weight_skill_type_name_overall <- cmath/totalrs
business$weight_skill_type_name_overall <- cbusiness/totalrs
comm$weight_skill_type_name_overall <- ccomm/totalrs
visual$weight_skill_type_name_overall <- cvisual/totalrs

program$rating_skill_type_overall <- program$rating * program$weight_skill_type_name_overall
math$rating_skill_type_overall <- math$rating * math$weight_skill_type_name_overall
business$rating_skill_type_overall <- business$rating * business$weight_skill_type_name_overall
comm$rating_skill_type_overall <- comm$rating * comm$weight_skill_type_name_overall
visual$rating_skill_type_overall <- visual$rating * visual$weight_skill_type_name_overall

dfrankskiltypeoverall <- rbind(program, math, business, comm, visual)
##############################################################################################################################################################

##WEIGHT BY SKILL SET NAME
rs <- subset(rs, rs$source_name != "RJMETRICS")
skillsetnames <- unique(rs$skill_set_name)

cs <- subset(rs, rs$skill_set_name == "Classical Statistics") 
isk <- subset(rs, rs$skill_set_name == "Industry-Specific Knowledge")
c <- subset(rs, rs$skill_set_name == "Communication")
ct <- subset(rs, rs$skill_set_name == "Creative Thinking")
b <- subset(rs, rs$skill_set_name == "Business")
pd <- subset(rs, rs$skill_set_name == "Product Development")
m <- subset(rs, rs$skill_set_name == "Math")
bdd <- subset(rs, rs$skill_set_name == "Big and Distributed Data")
bep <- subset(rs, rs$skill_set_name == "Back-End Programming")
ml <- subset(rs, rs$skill_set_name == "Machine Learning")
mp <- subset(rs, rs$skill_set_name == "Mathematical Programming")
sm <- subset(rs, rs$skill_set_name == "Surveys and Marketing")
gp <- subset(rs, rs$skill_set_name == "General Programming")
oop <- subset(rs, rs$skill_set_name == "Object-Oriented Programming")
sp <- subset(rs, rs$skill_set_name == "Statistical Programming")
st <- subset(rs, rs$skill_set_name == "Structured Data")
v <- subset(rs, rs$skill_set_name == "Visualization")
a <- subset(rs, rs$skill_set_name == "Algorithms")
is <- subset(rs, rs$skill_set_name == "Information Security")
ud <- subset(rs, rs$skill_set_name == "Unstructured Data")
sa <- subset(rs, rs$skill_set_name == "Systems Administration")
md <- subset(rs, rs$skill_set_name == "Mobile Devices")
gm <- subset(rs, rs$skill_set_name == "Graphical Models")
fep <- subset(rs, rs$skill_set_name == "Front-End Programming")
ts <- subset(rs, rs$skill_set_name == "Temporal Statistics")
o <- subset(rs, rs$skill_set_name == "Optimization")
s <- subset(rs, rs$skill_set_name == "Science")
bs <- subset(rs, rs$skill_set_name == "Bayesian/Monte-Carlo Statistics")
sim <- subset(rs, rs$skill_set_name == "Simulation")
ss <- subset(rs, rs$skill_set_name == "Spatial Statistics")
rd <- subset(rs, rs$skill_set_name == "Relational Databases")
dm <- subset(rs, rs$skill_set_name == "Data Manipulation")

total <- NROW(rs)

cs$weight_skill_set_name <- NROW(cs)/total
isk$weight_skill_set_name <- NROW(isk)/total
c$weight_skill_set_name <- NROW(c)/total
ct$weight_skill_set_name <- NROW(ct)/total
b$weight_skill_set_name <- NROW(b)/total
pd$weight_skill_set_name <- NROW(pd)/total
m$weight_skill_set_name <- NROW(m)/total
bdd$weight_skill_set_name <- NROW(bdd)/total
bep$weight_skill_set_name <- NROW(bep)/total
ml$weight_skill_set_name <- NROW(ml)/total
mp$weight_skill_set_name <- NROW(mp)/total
sm$weight_skill_set_name <- NROW(sm)/total
gp$weight_skill_set_name <- NROW(gp)/total
oop$weight_skill_set_name <- NROW(oop)/total
sp$weight_skill_set_name <- NROW(sp)/total
st$weight_skill_set_name <- NROW(st)/total
v$weight_skill_set_name <- NROW(v)/total
a$weight_skill_set_name <- NROW(a)/total
is$weight_skill_set_name <- NROW(is)/total
ud$weight_skill_set_name <- NROW(ud)/total
sa$weight_skill_set_name <- NROW(sa)/total
md$weight_skill_set_name <- NROW(md)/total
gm$weight_skill_set_name <- NROW(gm)/total
fep$weight_skill_set_name <- NROW(fep)/total
ts$weight_skill_set_name <- NROW(ts)/total
o$weight_skill_set_name <- NROW(o)/total
s$weight_skill_set_name <- NROW(s)/total
bs$weight_skill_set_name <- NROW(bs)/total
sim$weight_skill_set_name <- NROW(sim)/total
rd$weight_skill_set_name <- NROW(rd)/total
dm$weight_skill_set_name <- NROW(dm)/total
ss$weight_skill_set_name <- NROW(ss)/total

cs$ranking_skill_set_name <- cs$weight_skill_set_name * cs$rating
isk$ranking_skill_set_name <- isk$weight_skill_set_name * isk$rating
c$ranking_skill_set_name <- c$weight_skill_set_name * c$rating
ct$ranking_skill_set_name <- ct$weight_skill_set_name * ct$rating
b$ranking_skill_set_name <- b$weight_skill_set_name * b$rating
pd$ranking_skill_set_name <- pd$weight_skill_set_name * pd$rating
m$ranking_skill_set_name <- m$weight_skill_set_name * m$rating
bdd$ranking_skill_set_name <- bdd$weight_skill_set_name * bdd$rating
bep$ranking_skill_set_name <- bep$weight_skill_set_name * bep$rating
ml$ranking_skill_set_name <- ml$weight_skill_set_name * ml$rating
mp$ranking_skill_set_name <- mp$weight_skill_set_name * mp$rating
sm$ranking_skill_set_name <- sm$weight_skill_set_name * sm$rating
gp$ranking_skill_set_name <- gp$weight_skill_set_name * gp$rating
oop$ranking_skill_set_name <- oop$weight_skill_set_name * oop$rating
sp$ranking_skill_set_name <- sp$weight_skill_set_name * sp$rating
st$ranking_skill_set_name <- st$weight_skill_set_name * st$rating
v$ranking_skill_set_name <- v$weight_skill_set_name * v$rating
a$ranking_skill_set_name <- a$weight_skill_set_name * a$rating
is$ranking_skill_set_name <- is$weight_skill_set_name * is$rating
ud$ranking_skill_set_name <- ud$weight_skill_set_name * ud$rating
sa$ranking_skill_set_name <- sa$weight_skill_set_name * sa$rating
md$ranking_skill_set_name <- md$weight_skill_set_name * md$rating
gm$ranking_skill_set_name <- gm$weight_skill_set_name * gm$rating
fep$ranking_skill_set_name <- fep$weight_skill_set_name * fep$rating
ts$ranking_skill_set_name <- ts$weight_skill_set_name * ts$rating
o$ranking_skill_set_name <- o$weight_skill_set_name * o$rating
s$ranking_skill_set_name <- s$weight_skill_set_name * s$rating
bs$ranking_skill_set_name <- bs$weight_skill_set_name * bs$rating
sim$ranking_skill_set_name <- sim$weight_skill_set_name * sim$rating
rd$ranking_skill_set_name <- rd$weight_skill_set_name * rd$rating
dm$ranking_skill_set_name <- dm$weight_skill_set_name * dm$rating
ss$ranking_skill_set_name <- ss$weight_skill_set_name * ss$rating

dfrankskillsetname <- rbind(cs, isk, c, ct, b, pd, m, bdd, bep, ml, mp, sm, gp, sp, oop, st, v, a, is, ud, sa, md, gm, fep, ts, o, s, bs, sim, rd, dm, ss)

##############################################################################
## MySQL DB Section
##############################################################################

# Limit the 3 dataframes to skill_id, source_id, and the ratings values

df1 <- select(dfrankskiltypeoverall, skill_id, source_id, rating_skill_type_overall)
df2 <- select(dfrankskilltypesource, skill_id, source_id, rating_skill_type_source)
df3 <- select(dfrankskillsetname, skill_id, source_id, ranking_skill_set_name)

# Combine the three dataframes into one table joining on skill_id and source_id

update_df <- 
  df1 %>% 
  inner_join(df2, by=c("skill_id"="skill_id", "source_id"="source_id")) %>% 
  inner_join(df3, by=c("skill_id"="skill_id", "source_id"="source_id") )

dim(update_df)

str(update_df)

# push the dataframe into a temp table in MySQL
dbWriteTable(skilldb, name="df_temp", value=update_df)

# Null out any existing Rating values in the DB
dbSendQuery(con, "
            UPDATE TBL_DATA  
            SET WEIGHTED_RATING_OVERALL = NULL,
            WEIGHTED_RATING_BY_SKILL_TYPE = NULL,
            WEIGHTED_RATING_BY_SKILL_SET = NULL;")

# Update the rating values in tbl_data using the values in the temp table
dbSendQuery(con, "
            UPDATE TBL_DATA T, DF_TEMP R 
            SET T.WEIGHTED_RATING_OVERALL = R.T.WEIGHTED_RATING_OVERALL,
            T.WEIGHTED_RATING_BY_SKILL_TYPE =  R.WEIGHTED_RATING_BY_SKILL_TYPE,
            T.WEIGHTED_RATING_BY_SKILL_SET  = R.WEIGHTED_RATING_BY_SKILL_SET
            WHERE T.SKILL_ID = R.SKILL_ID 
            AND T.SOURCE_ID = R.SOURCE_ID;")


dbSendQuery(mydb, "DROP TABLE IF EXISTS DF_TEMP;")


# logic to check that updates happened




