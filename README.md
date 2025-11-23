# databricks-volume-to-git
A way to move files from databricks volume storage to GIT without the use of databricks repo management

This script depends on using GITHUB Person access tokens and databricks tokens.
Both can be limited time only use of tokens.
This script will first clone the Repo required in the .env file and configured outside of the script to run on a schedule only pushing the changes using checksums to ensure difference in files.

To run, enter your details in the example.env file and rename it to ".env" 
Checksums are saved in ./logs/.checksums so when testing remove checksums between runs to allow the same files to be uploaded.
This requires a paid databricks service and the volume being created in databricks before running this script.

The purpose of the script is to get log files from databricks into a git repo for ML/AL functions / any databricks functions to get logs.

To do:
Rework script for SSH keys instead of PATs. 
Rework functions to make it more modular with the repi git-repo-to-databricks https://github.com/TWhitwell/git-repo-to-databricks
