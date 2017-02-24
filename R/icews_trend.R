library(yaml)
library(RPostgreSQL)
library(httr)
library(tidyverse)

source("~/Projects/cameo/cameo.R")

cameo <- cameo_dict()
cameo_codes <- cameo$codes()
cameo_categories <- cameo$categories()

CONFIG_FILE <- "config/database.yaml"

config <- yaml.load_file(CONFIG_FILE)
url <- parse_url(config$local$url)

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

events_sample <- icews_events %>%
  filter(EventDate == "1995-01-01") %>%
  collect()

grouped_codes <- icews_events %>%
  group_by(Country, Year, Month, CAMEOCode) %>%
  summarise(Count = n()) %>%
  collect(n = Inf) 
  
grouped_categories <- grouped_codes %>%
  left_join(cameo_codes, by = c("CAMEOCode" = "EventCode")) %>%
  select(Country, Year, Month, EventCode = CAMEOCode, EventCategory, Count) %>%
  filter(!is.na(EventCategory)) %>%
  group_by(Country, Year, Month, EventCategory) %>%
  summarise(Count = sum(Count))

grouped_behaviors <- grouped_categories %>%
  left_join(cameo_categories, by = "EventCategory") %>%
  group_by(Country, Year, Month, BehaviorLevel) %>%
  summarise(Count = sum(Count))
    
behavior_counts <- grouped_behaviors %>%
  mutate(BehaviorLevel = paste0("Behavior_", BehaviorLevel)) %>%
  spread(BehaviorLevel, Count)

behavior_counts %>%
  mutate(EventCount = sum(Behavior_Material_Conflict, 
                          Behavior_Material_Cooperation, 
                          Behavior_Verbal_Conflict, 
                          Behavior_Verbal_Cooperation, na.rm = TRUE)) %>%
  mutate(Behavior_Material_Conflict    = Behavior_Material_Conflict / EventCount) %>%
  mutate(Behavior_Material_Cooperation = Behavior_Material_Cooperation / EventCount) %>%
  mutate(Behavior_Verbal_Conflict      = Behavior_Verbal_Conflict / EventCount) %>%
  mutate(Behavior_Verbal_Cooperation   = Behavior_Verbal_Cooperation / EventCount) 


