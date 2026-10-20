import pkgutil
import importlib
import inspect
import os
import sys

def run_examples_in(modules: list[str], class_name: str | None = None):
    example_classes: list[type] = []

    dir_containing_this_module = os.path.dirname(os.path.abspath(__file__))

    for folder in modules:
        all_packages = pkgutil.walk_packages([
            dir_containing_this_module + "/" + folder.replace(".", "/"),
        ])
        for package in all_packages:
            print(package)
            module = importlib.import_module("." + package.name, ".examples." + folder)
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if obj.__module__ != module.__name__:
                    continue
                # TODO - comparing the same class imported two different ways fails
                # There might a better way to do this
                if "Example" not in [obj.__name__ for obj in obj.__mro__]:
                    continue
                if not hasattr(obj, "run") or not callable(getattr(obj, "run")):
                    # Some classes that inherit from Example base class are "abstract" classes
                    continue

                # Now we know this class is a valid example
                # TODO: there's probably a way to get a specific class example in O(1) instead of O(n)
                if class_name and name != class_name:
                    continue

                print("Class found:", obj)
                example_classes.append(obj)

    print("Running examples...")
    for cls in example_classes:
        print(cls)
        print()

        example = cls()
        try:
            example.run()
        finally:
            example.cleanup()

if len(sys.argv) < 2:
    print("Missing arguments: EE/CE")
    exit(1)

if sys.argv[1] == "CE":
    modules = [
        "client",
        "string_ops"
    ]
else:
    modules = [
        "client.admin"
    ]

run_examples_in(modules)
