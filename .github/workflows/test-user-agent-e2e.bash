if [[ $# -lt 1 ]]; then
    echo "Usage: ./test-user-agent-e2e.bash (true|false) [<app-id>]"
    echo "First argument is whether to have client log in with a username"
    echo "Second argument is the app id string to pass to the client"
    exit 1
fi

set -x

python_background_script_name=run-client-in-bg.py

# First arg (bool): should client config app_id be set?
# Second arg (bool): should client log in with username and password?

cat <<-EOF >> $python_background_script_name
import aerospike
import time
import sys
config = {
    "hosts": [
        ("127.0.0.1", 3000)
    ]
}
if sys.argv[1] == "true":
    config["user"] = "superuser"
    config["password"] = "superuser"
if len(sys.argv) == 2:
    config["app_id"] = sys.argv[1]
client = aerospike.client(config)
while True:
    time.sleep(1)
EOF

# This shell will be closed once this step completes
python3 "$python_background_script_name" "$@" &
# TODO: We want to check if the python script raises a syntax error or not. (should fail immediately)
# When background processes fail, this step won't fail.

use_security_credentials="$1"

if [[ "$use_security_credentials" == "true" ]]; then
    CREDENTIALS="-U superuser -P superuser"
fi

server_version=$(docker run --network host aerospike/aerospike-tools asinfo $CREDENTIALS -v "build")
major_num=$(echo $server_version | cut -d '.' -f 1)
minor_num=$(echo $server_version | cut -d '.' -f 2)
if [[ $major_num -lt 8 || ( $major_num -eq 8 && $minor_num -lt 1 ) ]]; then
    echo "Server version $server_version doesn't support user-agent."
    exit 0
fi

info_cmd_response=$(docker run --network host aerospike/aerospike-tools asinfo $CREDENTIALS -v "user-agents" | head -n 1)
echo "Info command response: $info_cmd_response"
# Response format: user-agent=<base64 encoded string>:...
user_agent_base64_encoded=$(echo $info_cmd_response | perl -n -E 'say $1 if m/user-agent= ([a-zA-Z0-9+\/=]+) :/x')
echo $user_agent_base64_encoded
user_agent=$(echo $user_agent_base64_encoded | base64 -d)
echo $user_agent
# TODO: combine into one regex
# User agent format: <format-version>,<client language>-<version>,...
client_language=$(echo $user_agent | perl -n -E 'say $1 if m/[0-9]+, ([a-z]+) -/x')
client_version=$(echo $user_agent | perl -n -E 'say $1 if m/[0-9]+,[a-z]+- ([0-9.a-zA-Z+]+),/x')
echo $client_language
echo $client_version

test "$client_language" = "python"

# Client version from user agent
expected_client_version=$(python3 -m pip show aerospike | grep -i version | cut -d " " -f 2-)
test "$client_version" = "$expected_client_version"

app_id=$(echo $user_agent | perl -n -E 'say $1 if m/ ,([a-z\-]+)$ /x')
if [[ $# -eq 2 ]]; then
    # app_id was explicitly set in client config
    expected_app_id="$2"
elif [[ "$use_security_credentials" == "true" ]]; then
    expected_app_id="superuser"
else
    expected_app_id="not-set"
fi

test "$app_id" = "$expected_app_id"
