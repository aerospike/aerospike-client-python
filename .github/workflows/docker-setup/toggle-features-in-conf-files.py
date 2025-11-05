from jinja2 import Environment, FileSystemLoader
import os

AEROSPIKE_CONF_PATH = os.getenv("AEROSPIKE_CONF_PATH")
AEROSPIKE_CONF_FOLDER = os.path.dirname(AEROSPIKE_CONF_PATH)
env = Environment(loader = FileSystemLoader(AEROSPIKE_CONF_FOLDER), trim_blocks=True, lstrip_blocks=True)
# By default, all features enabled
# Disable feature if env var is present
env_vars = [
    "NO_SECURITY",
    "NO_TLS",
    "NO_SC"
]
kwargs = {}
for env_var in env_vars:
    value = os.getenv(env_var)
    # Determine which features to enable in Jinja template
    # Our jinja template checks if a feature is True in order to set a feature
    # e.g env var "NO_SC" -> Jinja variable "sc"
    jinja_var = env_var.replace("NO_", "").lower()
    # Enable a feature if env var is not set
    # e.g If env var NO_SC is not set, set Jinja variable "sc" to True
    # otherwise, if NO_SC is set, set "sc" to False
    kwargs[jinja_var] = value is None

# For debugging: print which features are enabled at runtime (entrypoint script will run this)
print(kwargs)

templates = [
    "astools.conf.jinja",
    "aerospike-dev.conf.jinja"
]
for tmpl_name in templates:
    template = env.get_template(tmpl_name)
    output = template.render(**kwargs)
    with open(f"{AEROSPIKE_CONF_FOLDER}/{tmpl_name}".removesuffix(".jinja"), "w") as f:
        f.write(output)
