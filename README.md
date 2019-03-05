# YouTube search

Our research examines how political parties and other actors communicate during elections.

This script takes a list of keywords and searches YouTube for matches. It saves results to Google BigQuery. You can configure the type of search and set frequency according to your needs. Note that in accordance with YouTube API rules, data saved to BigQuery expires in 14 days.

## Usage

Create a CSV file with at least two columns: keyword, study_group. Our standard input CSV contains a list of all Australian Federal candidates and electorates.

Copy config_default.py to config_local.py, and fill in your values as appropriate.

Add your YouTube API key to config_local.py, in the format:
DEVELOPER_KEY = "<YOUR_API_KEY_HERE>"

Also add the file path to your BigQuery JSON key, and fill in the appropriate values for the table you want to save results to.



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
