# Automatically abort script on errors
option batch abort
# Disable overwrite confirmations that conflict with the previous
option confirm off
# Connect
open sftp://%3%:%4%@%5%
# Force binary mode transfer
option transfer binary
# Upload the file to current working directory
#   % 2 % - full path for file to be transferred
#   % 1 % - passed in file name
put %2%\%1%
# Disconnect
close
# Exit WinSCP
exit