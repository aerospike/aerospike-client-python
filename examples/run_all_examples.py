import pkgutil
import importlib
import inspect
import os
import sys


example_classes: list[type] = []

dir_containing_this_module = os.path.dirname(os.path.abspath(__file__))

for folder in ["client", "string_ops"]:
    all_packages = pkgutil.walk_packages([
        dir_containing_this_module + "/" + folder,
    ])
    for package in all_packages:
        print(package)
        module = importlib.import_module("." + package.name, ".examples." + folder)
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue
            # TODO - comparing the same class imported two different ways fails
            # There might a better way to do this
            print("Found class has these base classes:", obj.__mro__)
            if "Example" not in [obj.__name__ for obj in obj.__mro__]:
                continue
            if not hasattr(obj, "run") or not callable(getattr(obj, "run")):
                # Some classes that inherit from Example base class are "abstract" classes
                continue

            print("Class found:", obj)
            example_classes.append(obj)

if len(sys.argv) == 2:
    example_classes = [cls for cls in example_classes if cls.__name__ == sys.argv[1]]

print("Running examples...")
for cls in example_classes:
    print(cls)
    example = cls().run()
