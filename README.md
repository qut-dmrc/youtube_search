# YouTube search

Our research examines how political parties and other actors communicate during elections.

This script takes a list of keywords and searches YouTube for matches. 

## Usage

Create a CSV file with at least two columns: keyword, study_group. Our standard input CSV contains a list of all Australian Federal candidates and electorates.

Add your YouTube API key to config_local.py, in the format:
DEVELOPER_KEY = "<YOUR_API_KEY_HERE>"

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
