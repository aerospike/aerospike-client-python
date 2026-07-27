import pkgutil
import importlib
import inspect
import os
import sys
# from . import Example


example_classes: list[type] = []

dir_containing_this_module = os.path.dirname(os.path.abspath(__file__))
all_packages = pkgutil.walk_packages([dir_containing_this_module + "/client"])
for package in all_packages:
    print(package)
    module = importlib.import_module("." + package.name, ".examples.client")
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if obj.__module__ != module.__name__:
            continue
        print("Class found:", obj)
        # print(Example is obj.__bases__[0])
        # TODO - comparing the same class imported two different ways fails
        # There might a better way to do this
        if obj.__bases__[0].__name__ != "Example":
            continue
        example_classes.append(obj)

if len(sys.argv) == 2:
    example_classes = [cls for cls in example_classes if cls.__name__ == sys.argv[1]]

print("Running examples...")
for cls in example_classes:
    print(cls)
    example = cls().run()
