## Setup
The big data sets are too big to be in a git repository. Please add them to the `data` folder yourself.
In the submission this has already been done.

## 1.1 How to run
```py
python assignment_[ASSIGNMENT_NUMBER].py

# When you want to use the big data sets
python assignment_[ASSIGNMENT_NUMBER].py false
```

## 1.1.1 How often a song was listened in a certain month of a particular year.
We add columns for the specific month and year to look for. Then the mapper evaluates whether month and year are equal to the month and year of the song play history, and only returns those values. The reducers reduces the sum of occurrences per track id. The framework makes sure of the sorting by track_id because it is the first value of the tuple the reducers returns.

## 1.1.2 For each user the hour of the day the listened to the most music.
We publish both `playhistory.csv` and `people.csv` to the mapper. The mapper takes the user id as key and for `playhistory.csv` the hour as value, and for `people.csv` the first and last name as values. The reducer then checks if value is int and adds all hours of a specific user to a list. Else if the value is a tuple then the first and last name are assigned. Finally the reducer checks what value of hour is most common in this list, and return first and last name and the hour that is most common and how often that hour occurred.

## 1.1.3 Top 5 songs played most often in specific hour of the day.
For this assignment, we ran into the problem that there are commas in the song titles in the tracks.csv file, which can break the CSV format. To handle this, we used a regular expression (regex) to correctly separate the columns for track data, including song titles that contain commas.

Mapper:

It processes both the playhistory.csv and tracks.csv files.
From playhistory.csv, we extract track_id, user_id, and the play date. We filter the data to include only records where the play occurred in a specified hour (e.g., 8 AM).
From tracks.csv, the mapper extracts track_id, artist, and title using a regex that properly handles quotes and commas in the title field.
Reducer:

The reducer aggregates play counts by track_id, summing the occurrences.
It returns the total play count, along with the track_id, song_name, and artist.
Execution:

The MapReduce framework processes the data, and the top 5 most played songs in the specified hour are output.
This method ensures accurate processing of song play counts, even when song titles contain commas, and efficiently aggregates the results for the top 5 tracks.

## 1.1.4 How often a song was listened in a certain month of a particular year.
The mapper filters and processes the data by comparing the year and month of the song's play history with the specified year and month. For each song play entry, the mapper evaluates whether the song was played in the target month and year. If the song's play history matches the given month and year, it returns a key-value pair with the track ID as the key and 1 as the value, indicating one occurrence of the song being played.

The reducer aggregates the occurrences of each song by summing up the values returned by the mapper. The reducer groups the values by track_id, calculates the total number of plays for each song, and returns a tuple containing the track ID and the total play count.


## 1.2 
The main function in 1.2 reads all files in the data directory, and gives each row to the mapper. The mapper splits each row into words and returns the word as key, and the filename and rownumber as value. The reducer is only used to join all the values of a key and returns them. Then the main loop writes the result to a text file.

## 1.3 How often each ip was hit in a certain month.
Each line in the log file has the ip at the start and the date in square brackets. In the mapper, we use a regex to group these values out of the line. Since there are a lot of lines, we increase the MapReduce workers to 16 and precompile the regex pattern. For ease of use and sorting, we convert the string month into its number equivelent. We give the month as key and ip as value to the reducer. The reducer now already neatly has all the ips of the given month. As a line is equivelent to a hit, we can just count the duplicate ips together the get the hit count. The result is saved in a text file as it would take a long time to print all lines to a console.

The main challange of this exercise was getting a working regex pattern and adding optimizations to make the huge data file processable.
