# Sphinx Documentation

You can use [Sphinx](http://sphinx-doc.org/index.html) to generate
documentation from the _rst_ pages in this directory.

## Install Sphinx

```bash
pip install Sphinx
```

## Building the Documentation

### HTML Documentation

```bash
mkdir -p ./htmldir
sphinx-build -b html . htmldir
```

Then open the index.html page in a browser:
```
cd htmldir && open index.html
```
