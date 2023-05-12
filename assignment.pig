--Register the piggybank jar
REGISTER 'hdfs:///tmp/piggybank.jar';

--Load the data with the headers
d_with_headers  = LOAD '/user/maria_dev/fortnite/FortniteStatistics.csv' using 
org.apache.pig.piggybank.storage.CSVExcelStorage() AS (
    date_str:chararray, 
    time_of_day_str:chararray, 
    placed:int, 
    mental_state:chararray,
    eliminations: int,
    assists: int,
    revives: int,
    accuracy_str: chararray,
    hits: int,
    headshots: int,
    distance_traveled: float,
    materials_gathered: int,
    materials_used: int,
    damage_taken: int,
    damage_done_to_players: int,
    damage_done_to_buildings: int
);

--Remove the headers from the data frame
d = FILTER d_with_headers BY date_str!='Date';

--create a formatted data frame 
formatted_data = FOREACH d GENERATE *, (int) REPLACE(accuracy_str, '%', '') AS accuracy:int;

--group data by date strings (dirty option)
grouped_date_data = GROUP formatted_data BY date_str;

--get the length per date group
group_lengths = FOREACH grouped_date_data GENERATE group AS date_str, 
COUNT(formatted_data) AS group_length;

-- Order the groups by their length in descending order
ordered_groups = ORDER group_lengths BY group_length DESC;
DUMP ordered_groups;

--filter data to get all the data where accuracy is between 90 and 100
filtered_data = FILTER formatted_data BY accuracy >= 90 AND accuracy <= 100;

--calculate the sum of the damage done to players using the filtered_data data frame
total_damage_result = FOREACH (GROUP filtered_data ALL) GENERATE 
SUM(filtered_data.damage_done_to_players) AS total_damage;

--Display/dump the data to the screen
dump total_damage_result;

--Group the data by mental state
grouped_data = GROUP formatted_data BY mental_state;

--Create headshot data frame per group
grouped_data_result = FOREACH grouped_data GENERATE group AS mental_state, SUM(formatted_data.headshots) AS total_headshots;
dump grouped_data_result;