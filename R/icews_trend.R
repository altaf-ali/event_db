library(yaml)
library(RPostgreSQL)
library(httr)
library(dplyr)
library(ggplot2)

CONFIG_FILE <- "config/database.yaml"

config <- yaml.load_file(CONFIG_FILE)
url <- parse_url(config$url)

event_db <- src_postgres(url$path, 
                         url$hostname,
                         url$port,
                         url$username,
                         url$password)
      
icews_events <- tbl(event_db, "icews")

events <- icews_events %>%
  group_by(EventDate) %>%
  summarise(Count = n()) %>%
  collect() %>%
  mutate(EventDate = as.Date(EventDate)) 
  
ggplot(events, aes(EventDate, Count)) +
  ggtitle("Daily") +
  geom_line(color = "steelblue") +
  theme(axis.text.x = element_text(angle = 30),
        axis.title.x = element_blank())
        
events

