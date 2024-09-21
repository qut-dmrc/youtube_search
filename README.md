# ARCHIVED - YouTube removed required functionality

This tool hasn't worked since [YouTube broke the ability to search by date in 2019](https://medium.com/@nicsuzor/youtube-nukes-its-api-and-search-functionality-in-response-to-christchurch-massacre-6051b4f2bb77).


# YouTube search tools

This repository includes two scripts to search YouTube videos. 

* YouTube Search: a script to search for videos matching keywords.

* YouTube Sample: a script to generate a random sample of YouTube videos posted in the last few minutes

We use this data to examine how political parties and other actors communicate using YouTube.

These scripts save results to Google BigQuery. You can configure the type of search and set frequency according to your needs. Note that in accordance with YouTube API rules, data saved to BigQuery expires in 14 days.

## youtube_search Usage

Create a CSV file with at least two columns: keyword, study_group. Our standard input CSV contains a list of all Australian Federal candidates and electorates.

Copy config_default.yml to config.yml and fill with your values. 

Add your YouTube API key to the config file, and also add the file path to your BigQuery JSON key, and fill in the appropriate values for the table you want to save results to.

Run the program:

```
    youtube_search.py [-v] [-l log_file] [--search_results=s] [--search_type=type] <csv_input_file_name>

    Options:
      -h --help                 Show this screen.
      -v --verbose              Increase verbosity for debugging.
      -l <log_file> --log=<log_file>    Save log to file
      --search_results=s        Number of search results to save [default: 20]
      --search_type=type        Type of search (last-hour, top-rated, all-time, or today [default: today]

      --version  Show version.
```

## youtube_sample Usage

Copy config_default.yml to config.yml and fill with your values. 

Add your YouTube API key to the config file, and also add the file path to your BigQuery JSON key, and fill in the appropriate values for the table you want to save results to.

Note that the YouTube LIST API is not currently returning all the results that it should be - there are open bug reports, but the problem has not been fixed. See:

* https://support.google.com/youtube/thread/2494861?hl=en - showing that the problem first occurred in March 2019, and was fixed at one point
* https://support.google.com/youtube/thread/2915550?hl=en - showing that the problem still exists.
* See also https://digitalsocialcontract.net/youtube-nukes-its-api-and-search-functionality-in-response-to-christchurch-massacre-6051b4f2bb77 

Run the program:

```
    python3 youtube_sample.py 
```
