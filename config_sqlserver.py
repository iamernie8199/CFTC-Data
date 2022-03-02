# %%
import argparse
from box import Box
import yaml

# %%
parser = argparse.ArgumentParser(description='Process some integers.', prefix_chars='-')
parser.add_argument('-env', '--env', nargs=1)
parser.add_argument('-file', '--file', nargs=1)
parser.add_argument('-func', '--func', nargs=1)
args = parser.parse_args(["--env", "dev", "-file", "test.py", "--func", "testMain"])
config_loc = "config\\config.yml"
print("Config ENV", "\"" + args.env[0] + "\"", "loaded!")
with open(config_loc, "r") as ymlfile:
    global_cfg = Box(yaml.safe_load(ymlfile))[args.env[0]]

# %%
#global_cfg

# %%



