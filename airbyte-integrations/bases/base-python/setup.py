# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import setuptools

setuptools.setup(
    name="base-python",
    description="Contains machinery to make it easy to write an integration in python.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    url="https://github.com/airbytehq/airbyte",
    packages=setuptools.find_packages(),
    package_data={"": ["models/yaml/*.yaml"]},
    install_requires=[
        "PyYAML==5.4",
        "pydantic==1.6.1",
        "airbyte-protocol",
        "jsonschema==2.6.0",
        "requests==2.25.1",
        "backoff==1.10.0",
        "pytest",
        "pendulum",
    ],
    entry_points={
        "console_scripts": ["base-python=base_python.entrypoint:main"],
    },
    extras_require={
        "docs": [
            'sphinx',
            'myst_parser'  # To allow writing docs in markdown
        ],
    },
)
