from jinja2 import Environment, FileSystemLoader
import os

env = Environment(loader = FileSystemLoader('/etc/aerospike/'))
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
    jinja_var = env_var.replace("NO_", "").lower()
    kwargs[jinja_var] = value is None
# For debugging
print(kwargs)

templates = [
    "astools.conf.jinja",
    "aerospike-dev.conf.jinja"
]
for tmpl_name in templates:
    template = env.get_template(tmpl_name)
    output = template.render(**kwargs)
    print(output)
    with open(f"/etc/aerospike/{tmpl_name}".removesuffix(".jinja"), "w") as f:
        f.write(output)
