import pkgutil
import importlib
import inspect
import os
from . import Example


example_classes = []

dir_containing_this_module = os.path.dirname(os.path.abspath(__file__))
all_packages = pkgutil.walk_packages([dir_containing_this_module + "/client"])
for package in all_packages:
    print(package)
    importlib.import_module("." + package.name, ".examples.client")
    for name, obj in inspect.getmembers(package, inspect.isclass):
        if obj.__module__ != package.name:
            continue
        if not issubclass(obj, Example):
            continue
        example_classes.append(obj)

for cls in example_classes:
    example = cls().run()
