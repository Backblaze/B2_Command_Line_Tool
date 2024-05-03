######################################################################
#
# File: b2/_internal/_cli/const.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# Optional Env variable to use for getting account info while authorizing
B2_APPLICATION_KEY_ID_ENV_VAR = 'B2_APPLICATION_KEY_ID'
B2_APPLICATION_KEY_ENV_VAR = 'B2_APPLICATION_KEY'

# Optional Env variable to use for adding custom string to the User Agent
B2_USER_AGENT_APPEND_ENV_VAR = 'B2_USER_AGENT_APPEND'
B2_ENVIRONMENT_ENV_VAR = 'B2_ENVIRONMENT'
B2_DESTINATION_SSE_C_KEY_B64_ENV_VAR = 'B2_DESTINATION_SSE_C_KEY_B64'
B2_DESTINATION_SSE_C_KEY_ID_ENV_VAR = 'B2_DESTINATION_SSE_C_KEY_ID'
B2_SOURCE_SSE_C_KEY_B64_ENV_VAR = 'B2_SOURCE_SSE_C_KEY_B64'

# Threads defaults
DEFAULT_THREADS = 10

# Constants used in the B2 API
CREATE_BUCKET_TYPES = ('allPublic', 'allPrivate')

B2_ESCAPE_CONTROL_CHARACTERS = 'B2_ESCAPE_CONTROL_CHARACTERS'

# Set to 1 when running under B2 CLI as a Docker container
B2_CLI_DOCKER_ENV_VAR = 'B2_CLI_DOCKER'
