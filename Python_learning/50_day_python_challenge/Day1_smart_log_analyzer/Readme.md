# Smart Log Analyzer

we will parse a log file and generate useful insights.

## what it will do:
Build a script that:
 Reads a log file (server.log)
 Counts: Total lines ERROR / WARNING / INFO occurrences 
 Shows most frequent error message

## Analysis before coding

- the log file format ( Datetime Level Message)
    2025-01-01 09:00:22 ERROR Database connection failed

- Split the log message into Date, time, Level, Message

- Create a Dictonary for counting Logs by Level ERROR / WARNING / INFO occurrences 

- Show which Level repeat the most in log file