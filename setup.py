# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['exasol_advanced_analytics_framework']

package_data = \
{'': ['*']}

setup_kwargs = {
    'name': 'exasol-advanced-analytics-framework',
    'version': '0.1.0',
    'description': 'Framework for data analysis methodologies',
    'long_description': '',
    'author': 'Umit Buyuksahin',
    'author_email': 'umit.buyuksahin@exasol.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://github.com/exasol/advanced-analytics-framework',
    'packages': packages,
    'package_data': package_data,
    'python_requires': '>=3.6.1,<4.0',
}


setup(**setup_kwargs)
