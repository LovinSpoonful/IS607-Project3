

# SECTION 1 - csv files were converted to normalized database


#############################################################
#
# Read the csv data files and load to normalized database
# ROB
#############################################################

# Establish connection to the database
library(RODBC)
cnString <- "MySQL_ANSI;SERVER=localhost;DATABASE=skill;UID=root;PASSWORD=CUNYRBridge4!;OPTION=3;"
#cnString <- "MySQL_ANSI;SERVER=db4free.net;DATABASE=skill;UID=project3;PASSWORD=CUNYRBridge4;OPTION=3;"
db <- odbcConnect(cnString, case="nochange")

#manually input the list of csv files on github
sources <- c("https://raw.githubusercontent.com/LovinSpoonful/IS607-Project3/master/08-barman-data-cleaned.csv",
             "https://raw.githubusercontent.com/LovinSpoonful/IS607-Project3/master/09-briot-data-cleaned.csv",
             "https://raw.githubusercontent.com/LovinSpoonful/IS607-Project3/master/10-karr-data-cleaned.csv",
             "https://raw.githubusercontent.com/LovinSpoonful/IS607-Project3/master/11-fanelli-data-cleaned.csv")

# Import all the source data into the database
sqlQuery(db,"DELETE FROM tbl_import;") # first truncate the imported records table
sqlQuery(db,"DELETE FROM tbl_source;") # also empty the source table

# Loop through every source data file
for (i in 1:length(sources)){
  
  #record the source of the data in the source table
  dat <- read.csv(file = sources[i], header=FALSE, sep=",") 
  source_id <- i
  source_name <- as.character(dat[1,1])  # get the source name from the top row of the data file
  source_url <- as.character(dat[2,1])   # get the source URL from 2nd row
  sSQL <- paste("INSERT INTO tbl_source VALUES (", source_id, ", '", gsub(" ", "_", source_name), "', '", source_name, "', '", source_url, "')",   sep = "", collapse = NULL)  # write this information into the source table
  sqlQuery(db,sSQL)
  
  #export all the data for this source into the database (the Import table)
  dat <- dat[-1:-3,] #remove the header lines
  source_id <- rep(i,nrow(dat)) # create a column to show which data set these samples came from
  dat <- data.frame(source_id,dat) # add that column to the front of the dataframe
  names(dat) <- c("source_id","skill_type_name","skill_name","rating")  #hardcode the column names.  should get them from the table definition!
  sqlSave(db, dat, tablename = "tbl_import", rownames=FALSE, append=TRUE) # write the dataframe to the mySQL table
}


#De-duplicate the imported data and setup the Skill Types, Skill Names tables
# retrieve the imported records into a data frame
df <- sqlQuery(db,"SELECT source_id, skill_type_name, skill_name, ROUND(AVG(rating),3) rating FROM tbl_import GROUP BY source_id, skill_type_name, skill_name ORDER BY source_id, skill_type_name, skill_name;", stringsAsFactors = FALSE)  

# write the de-duplicated import records back into the database
sqlQuery(db,"DELETE FROM tbl_import;") # first truncate the imported records table
sqlSave(db, df, tablename = "tbl_import", rownames=FALSE, append=TRUE) # write the dataframe to the mySQL table

# create a master list of skill types (using r)
sqlQuery(db,"TRUNCATE TABLE tbl_data;") # first truncate it
sqlQuery(db,"TRUNCATE TABLE tbl_data_n;") # first truncate it
sqlQuery(db,"TRUNCATE TABLE tbl_skill;") # first truncate the imported records table
sqlQuery(db,"TRUNCATE TABLE tbl_skill_type") # first truncate the table
skill_type_name <- unique(df$skill_type_name,incomparables = FALSE, stringsAsFactors = FALSE)
skill_type_id <- c(1:length(skill_type_name))
skill_type_description <- skill_type_name # enrich later if can get descriptions
df1 <- data.frame(skill_type_id,skill_type_name, skill_type_description) # create a dataframe that matches the table structure
sqlSave(db, df1, tablename = "tbl_skill_type", rownames=FALSE, append=TRUE) # write the dataframe to the mySQL table

# create a master list of skill names (using my sql)
sqlQuery(db,"INSERT INTO tbl_skill (skill_id, skill_type_id, skill_name) SELECT NULL, st.skill_type_id, i.skill_name FROM tbl_import i JOIN tbl_skill_type st ON st.skill_type_name = i.skill_type_name GROUP BY st.skill_type_id, i.skill_name;")








#SECTION 2 -  skills were mapped to a master set of skill sets

##########################################################################################################
#KEITH F   SKILL SETS AND SKILL -- SET CROSS REFERENCE
# -------------------------------------------------------------
# This R code process the csv file Skill_Keyword_Map.csv into a dataframe
# with two columns: skill_category and skill_name
# 
# The input file is in the format of:
#
#     Skill Category,       Skill Keyword
#     --------------        --------------
#     Machine Learning,	    "decision trees, neural nets, SVM, clustering, predictive analytics, machine learning, clustering"
#
#
# The output will be a dataframe:
#  
#  skill_category           skill_keyword
#  --------------          ---------------
#  Machine Learning         decision trees
#  Machine Learning         neural nets
#  Machine Learning         SVM
#  Machine Learning         clustering
#  Machine Learning         predictive analytics
#  Machine Learning         machine learning
#  Machine Learning         clustering
#
#  This script produces the file called Skill_Category_Xref.csv (loaded to GitHub)
# -----------------------------------------------------------------------------------------

library(stringr)
library(RMySQL)
library(dplyr)

proj_user <- "root"
proj_pwd  <- "CUNYRBridge4!"
proj_db   <- "skill"
proj_host <- "localhost"
# proj_user <- "project3"
# proj_pwd  <- "CUNYRBridge4"
# proj_db   <- "skill"
# proj_host <- "db4free.net"

#############################################################
# Step 1 - File Parsing
# Parse the Skill_Keyword_Map file located on GitHub
#############################################################

# load in Skill_Keyword_Map.csv file from GitHub
# this file matches skill keywords with a skill set category

URL <- "https://raw.githubusercontent.com/LovinSpoonful/IS607-Project3/master/Skill_Keyword_Map.csv"

skill_map <- read.csv(URL, header=TRUE, stringsAsFactors = FALSE)[, 1:3]

#initialize an empty dataframe to rbind all the keywords in column format
ret <- data.frame(Raw.Skill = character(), Category = character(), Skill.Type = character())

for (i in 1:nrow(skill_map))  {
  
  keywords <- data.frame(str_split(skill_map$Skill.Keyword[i], ","))
  
  keywords$skill_category <- skill_map$Skill.Category[i]
  keywords$Skill.Type     <- skill_map$Skill.Type[i]
  
  colnames(keywords) <- c("Raw.Skill", "Category", "Skill.Type")
  
  ret <- rbind(ret, keywords)
  
}

# trim whitespace; convert factor to characater
ret$Raw.Skill <- str_trim(ret$Raw.Skill, side = "both")
ret$Raw.Skill <- as.character(ret$Raw.Skill)

str(ret)

# dedupe in case there are duplicate skills in the file
ret <- distinct(ret)

# create the output file which is the file Skill_Category_Xref.csv

# write.csv(ret, "Skill_Category_Xref.csv", row.names=FALSE)

################################################################################
# Step 2 - MySQL DB: Load tbl_skill_set 
# ------------------------------------------
# Using the parsed Skill-Map file, load the skill sets (categories)
# into tbl_skill_set
###############################################################################

# establish the connection to the skill DB on db4free.net
skilldb = dbConnect(MySQL(), user=proj_user, password=proj_pwd, dbname=proj_db, host=proj_host)

skill_set <- data.frame(skill_set_name = ret$Category, 
                        skill_set_description = NA, 
                        skill_type = ret$Skill.Type
)
# we only want the distinct skill_sets (categories) and skill_types
skill_set <- distinct(skill_set)

skill_set$skill_set_id <- rownames(skill_set)
skill_set[,"skill_set_description"] <- as.character(NA)

# convert everything to a character
skill_set[] <- lapply(skill_set, as.character)


# pull down skill types from MySQL
skill_types <-  dbGetQuery(skilldb, "select skill_type_id, skill_type_name from tbl_skill_type")

str(skill_types)

skill_set_insert <-
  skill_set %>%  
  inner_join(skill_types, by=c("skill_type" = "skill_type_name")) %>%
  select(skill_set_id, skill_type_id, skill_set_name, skill_set_description)


## delete the table tbl_skills_categories
del <- dbGetQuery(skilldb, "delete from tbl_skill_set")

# write the dataframe to the mySQL table
dbWriteTable(skilldb, value = skill_set_insert, name = "tbl_skill_set", append = TRUE, row.names = NA, header = FALSE) 

rs <- dbGetQuery(skilldb, "select count(*) from tbl_skill_set")

if (rs == 0) print("No Rows Loaded into tbl_skill_set!")


################################################################################
# Step 3 - MySQL DB: Load tbl_skill_set_xref
# ------------------------------------------
# Load tbl_skill_set_xref Using the parsed Skill-Map file for skills to skill set. 
# This DB tables associates a skill to a skill set based on the Skill-Map file.
###############################################################################

# pull down the skill table for the skill_id and name
skills <-  dbGetQuery(skilldb, "select skill_id, skill_name from tbl_skill")

# pull down the skill set table for the sKill_set_id and name
skill_sets <- dbGetQuery(skilldb, "select skill_set_id, skill_set_name from tbl_skill_set")

# use dplr to build the dataframe to insert into tbl_skill_set_xref
skill_set_xref_insert <- 
  ret %>% 
  inner_join(skills, by=c("Raw.Skill"="skill_name")) %>%
  inner_join(skill_sets, by=c("Category"="skill_set_name"))  %>%
  select(skill_set_id, skill_id)

str(skill_set_xref_insert)

## delete the table tbl_skills_categories
del <- dbGetQuery(skilldb, "delete from tbl_skill_set_xref")

# write the dataframe to the mySQL table
dbWriteTable(skilldb, value = skill_set_xref_insert, name = "tbl_skill_set_xref", append = TRUE, row.names = NA, header = FALSE) 

rs <- dbGetQuery(skilldb, "select count(*) from tbl_skill_set_xref")

if (rs == 0) print ("No Rows Loaded into tbl_skill_set_xref!")

# close the connection
dbDisconnect(skilldb)










# SECTION 3:  data was loaded into tbl_data and scaled

###########################################################################
#ROB

#Populate the normalized data table of all ratings values across all sources
sqlQuery(db,"DELETE FROM tbl_data_n;") # first truncate it
# populate the data table
sSQL <- paste("INSERT INTO tbl_data_n (skill_type_id, skill_set_id, skill_id, source_id, rating) ",
              "SELECT s.skill_type_id, sx.skill_set_id, s.skill_id, i.source_id, i.rating         ",
              "FROM tbl_skill s                                                  ",
              "JOIN tbl_skill_type st ON s.skill_type_id = st.skill_type_id      ",              
              "JOIN tbl_skill_set_xref sx ON s.skill_id = sx.skill_id            ",
              "JOIN tbl_import i ON i.skill_name = s.skill_name;                 ", sep = "")
sqlQuery(db,sSQL)

#To make it easier to create analysis, also create a denormalized table with descriptors
sqlQuery(db,"DELETE FROM tbl_data;") # first truncate it
# populate the table
sSQL <- paste(
  "INSERT INTO tbl_data (skill_type_id, skill_set_id, skill_id, source_id, skill_type_name, skill_set_name, skill_name, source_name, rating) ",
  "SELECT d.skill_type_id, d.skill_set_id, d.skill_id, d.source_id, st.skill_type_name, ss.skill_set_name, s.skill_name, so.source_name, d.rating ", 
  "FROM tbl_data_n d ", 
  "JOIN tbl_skill_type st ON d.skill_type_id = st.skill_type_id ", 
  "JOIN tbl_skill_set  ss ON d.skill_set_id  = ss.skill_set_id ",  
  "JOIN tbl_skill       s ON d.skill_id      =  s.skill_id ",
  "JOIN tbl_source     so ON d.source_id     = so.source_id;", sep = "")
sqlQuery(db,sSQL)  

#copy the skill set id to the skill table 
sSQL <- "UPDATE tbl_skill s JOIN tbl_skill_set_xref sx  ON sx.skill_id = s.skill_id  SET s.skill_set_id = sx.skill_set_id;"
sqlQuery(db,sSQL)  

skilldb = dbConnect(MySQL(), user=proj_user, password=proj_pwd, dbname=proj_db, host=proj_host)

#calculate the max and min ratings within each source and post to temporary table
df <- dbGetQuery(skilldb, "SELECT source_id, MAX(rating) rating_max, MIN(rating) rating_min FROM tbl_data GROUP BY source_id;")
dbSendQuery(skilldb, "DROP TABLE IF EXISTS df_temp;")
dbWriteTable(skilldb, name="df_temp", value=df)

#scale the ratings from 0 to 100
dbSendQuery(skilldb, "UPDATE tbl_data SET rating_scalar = NULL;")
dbSendQuery(skilldb, "
            UPDATE tbl_data d, df_temp t 
            SET d.rating_scalar = ROUND((d.rating - t.rating_min) / (t.rating_max - t.rating_min),2)*100 
            WHERE d.source_id = t.source_id;")

#since this is a discrete series, set all the minimum values to one, instead of zero
dbSendQuery(skilldb, "UPDATE tbl_data SET rating_scalar = 1 WHERE rating_scalar < 1;")









# SECTION 4: data was weight ranked 

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
#a$weight_skill_set_name <- NROW(a)/total
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
#a$ranking_skill_set_name <- a$weight_skill_set_name * a$rating
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

#dfrankskillsetname <- rbind(cs, isk, c, ct, b, pd, m, bdd, bep, ml, mp, sm, gp, sp, oop, st, v, a, is, ud, sa, md, gm, fep, ts, o, s, bs, sim, rd, dm, ss)
dfrankskillsetname <- rbind(cs, isk, c, ct, b, pd, m, bdd, bep, ml, mp, sm, gp, sp, oop, st, v, is, ud, sa, md, gm, fep, ts, o, s, bs, sim, rd, dm, ss)


##############################################################################
## KEITH F
## MySQL DB Section
##############################################################################

# Limit the 3 dataframes to skill_id, source_id, and the ratings values

df1 <- select(dfrankskiltypeoverall, skill_id, source_id, rating_skill_type_overall)
df2 <- select(dfrankskilltypesource, skill_id, source_id, rating_skill_type_source)
df3 <- select(dfrankskillsetname, skill_id, source_id, ranking_skill_set_name)

# Combine the three dataframes into one table joining on skill_id and source_id
#df1 <- merge(df1, df2, by=c("skill_id"="skill_id", "source_id"="source_id")) # alternate method
#df1 <- merge(df1, df3, by=c("skill_id"="skill_id", "source_id"="source_id"))
update_df <- 
  df1 %>% 
  inner_join(df2, by=c("skill_id"="skill_id", "source_id"="source_id")) %>% 
  inner_join(df3, by=c("skill_id"="skill_id", "source_id"="source_id") )
update_df

dim(update_df)
str(update_df)

# push the dataframe into a temp table in MySQL
dbSendQuery(skilldb, "DROP TABLE IF EXISTS df_temp;")
dbWriteTable(skilldb, name="df_temp", value=update_df)

# Null out any existing Rating values in the DB
dbSendQuery(skilldb, "
            UPDATE tbl_data  
            SET WEIGHTED_RATING_OVERALL = NULL,
            WEIGHTED_RATING_BY_SKILL_TYPE = NULL,
            WEIGHTED_RATING_BY_SKILL_SET = NULL;")

# Update the rating values in tbl_data using the values in the temp table
dbSendQuery(skilldb, "
            UPDATE tbl_data T, df_temp R 
            SET T.WEIGHTED_RATING_OVERALL = R.rating_skill_type_overall,
            T.WEIGHTED_RATING_BY_SKILL_TYPE =  R.rating_skill_type_source,
            T.WEIGHTED_RATING_BY_SKILL_SET  = R.ranking_skill_set_name
            WHERE T.SKILL_ID = R.SKILL_ID 
            AND T.SOURCE_ID = R.SOURCE_ID;")

dbSendQuery(skilldb, "DROP TABLE IF EXISTS df_temp;")

#close the connection
dbDisconnect(skilldb)









# SECTION 5:  presentation-ready views were created (using scalar values) 


##############################################################################

#ROB
# establish the connection to the skill DB on db4free.net
skilldb = dbConnect(MySQL(), user=proj_user, password=proj_pwd, dbname=proj_db, host=proj_host)

# make presentable views
dbSendQuery(skilldb, "DROP VIEW IF EXISTS vw_skill_type_frequency_percent;")
dbSendQuery(skilldb,"
            CREATE VIEW `vw_skill_type_frequency_percent` AS
            SELECT skill_type_name, ROUND(sum(rating_scalar) / (SELECT SUM(rating_scalar) FROM tbl_data),2)*100 AS Frequency_Percent
            FROM tbl_data
            GROUP BY skill_type_name ORDER BY sum(rating_scalar) DESC;")

dbSendQuery(skilldb, "DROP VIEW IF EXISTS vw_top10_skills_overall;")
dbSendQuery(skilldb, "
            CREATE VIEW `vw_top10_skills_overall` AS
            SELECT skill_name, SUM(rating_scalar) 
            FROM tbl_data 
            GROUP BY skill_name 
            ORDER BY SUM(rating_scalar) DESC LIMIT 10;")

dbSendQuery(skilldb, "DROP VIEW IF EXISTS vw_top10_skills_overall_excluding_rjmetrics;")
dbSendQuery(skilldb, "
            CREATE VIEW `vw_top10_skills_overall_excluding_rjmetrics` AS
            SELECT skill_name, SUM(rating_scalar) 
            FROM tbl_data
            WHERE source_name <> 'RJMETRICS'
            GROUP BY skill_name 
            ORDER BY SUM(rating_scalar) DESC LIMIT 10;")

dbSendQuery(skilldb, "DROP VIEW IF EXISTS vw_top10_skill_sets_overall;")
dbSendQuery(skilldb, "
            CREATE VIEW `vw_top10_skill_sets_overall` AS
            SELECT skill_set_name, SUM(rating_scalar) 
            FROM tbl_data 
            GROUP BY skill_set_name 
            ORDER BY SUM(rating_scalar) DESC LIMIT 10;")

dbSendQuery(skilldb, "DROP VIEW IF EXISTS vw_bottom10_skills_overall;")
dbSendQuery(skilldb, "
            CREATE VIEW `vw_bottom10_skills_overall` AS
            SELECT skill_name, SUM(rating_scalar) 
            FROM tbl_data 
            GROUP BY skill_name 
            ORDER BY SUM(rating_scalar) LIMIT 10;")

dbSendQuery(skilldb, "DROP VIEW IF EXISTS vw_bottom10_skill_sets_overall;")
dbSendQuery(skilldb, "
            CREATE VIEW `vw_bottom10_skill_sets_overall` AS
            SELECT skill_set_name, SUM(rating_scalar) 
            FROM tbl_data 
            GROUP BY skill_set_name 
            ORDER BY SUM(rating_scalar) LIMIT 10;")


# Create each complex view (rankings within categories)
parent  <- c("skill_set_name","source_name", "skill_type_name")
child   <- c("skill_name","skill_name", "skill_set_name")
top_btm <- c("top","top", "top")
label   <- c("skills_by_skill_set","skills_by_source", "skill_sets_by_skill_type")
reps    <- c(3,10,5)

for (i in 1:length(parent)){
  sSQL <-  paste("SELECT ", parent[i], ", ", child[i], ", SUM(rating_scalar) rating_scalar  FROM tbl_data  GROUP BY ", parent[i], ", ", child[i], ";", sep = "")
  df <- dbGetQuery(skilldb, sSQL)
  dbSendQuery(skilldb, "DROP TABLE IF EXISTS df_temp;")
  dbWriteTable(skilldb, name="df_temp", value=df)
  sSQL <- paste("SELECT ", parent[i], ", ", child[i], 
                ", rating_scalar, rank FROM (SELECT ", parent[i], ", ", child[i], 
                ", rating_scalar, @rank := IF(@current = ", parent[i], ", @rank+1,1) AS rank, @current := ", parent[i], 
                " FROM df_temp ORDER BY ", parent[i], ", rating_scalar DESC) ranked WHERE rank <= ", reps[i], ";", sep = "")
  df <- dbGetQuery(skilldb, sSQL)
  df <- dbGetQuery(skilldb, sSQL) # sometimes the ranks don't work the first time, just repeat and they do!!!!??!!!
  obj <- paste(top_btm[i],reps[i],"_", label[i], sep = "") # name of table or view
  dbSendQuery(skilldb, paste("DROP TABLE IF EXISTS tbl_", obj, ";", sep = ""))
  dbWriteTable(skilldb, name=paste("tbl_", obj, sep = ""), value=df) #create a temp table for this view  (can't create views with variables)
  dbSendQuery(skilldb, paste("DROP VIEW IF EXISTS vw_", obj, ";", sep = "")) # drop the view
  sSQL = paste("CREATE VIEW `vw_", obj, "` AS SELECT * FROM tbl_", obj, " ORDER BY ", parent[i], ", rank;", sep = "") # create syntax to recreate the view
  dbSendQuery(skilldb, sSQL)
}


#Show how to query the data
df <- sqlQuery(db,"SELECT * FROM tbl_data;")
head(df)
dbDisconnect(skilldb)


