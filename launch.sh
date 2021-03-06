#!/bin/bash

source /environment.sh

# initialize launch file
dt_launchfile_init

# YOUR CODE BELOW THIS LINE
# ----------------------------------------------------------------------------


# NOTE: Use the variable CODE_DIR to know the absolute path to your code
# NOTE: Use `dt_exec COMMAND` to run the main process (blocking process)

# install avahi services
dt_install_all_services

# launching app
dt_exec python3 -m "device_loader.launch"


# ----------------------------------------------------------------------------
# YOUR CODE ABOVE THIS LINE

# terminate launch file
dt_launchfile_terminate
