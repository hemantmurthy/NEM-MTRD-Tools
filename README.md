# MTRD File Stats and Posting Tool

This is a tool to analyse MTRD files (ASEXml format) placed in a directory/folder and produce stats on the same.
The tool can also be used to post MTRD files to an API. 

## How to use
The tool can be run using the command

`your-command-prompt> java hamy/PostMTRDFiles`

The tool will prompt you for a set of inputs
* The API URL to which files will be POSTed to
* The JMS Destination Name
* The source folder/directory where files to be analysed/posted are stored
* If a proxy connection is needed to post files
* The number of threads on which files will be posted
* Transaction types to be processed (NEM12, NEM13 or TACK). Specify one or more separated by space
