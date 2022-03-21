# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['exasol_advanced_analytics_framework']

package_data = \
{'': ['*'], 'exasol_advanced_analytics_framework': ['lua/src/*', 'lua/test/*']}

setup_kwargs = {
    'name': 'exasol-advanced-analytics-framework',
    'version': '0.1.0',
    'description': 'Framework for building complex data analysis algorithms with Exasol',
    'long_description': '# Exasol Advanced Analytics Framework\n\n**This project is at an early development stage.**\n\nFramework for building complex data analysis algorithms with Exasol.\n\n\n## Information for Users\n\n- [User Guide](doc/user_guide/user_guide.md)\n- [System Requirements](doc/system_requirements.md)\n- [Design](doc/design.md)\n- [License](LICENSE)\n\n## Information for Developers\n\n- [Developers Guide](doc/developer_guide/developer_guide.md)\n\n',
    'author': 'Umit Buyuksahin',
    'author_email': 'umit.buyuksahin@exasol.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://github.com/exasol/advanced-analytics-framework',
    'packages': packages,
    'package_data': package_data,
    'python_requires': '>=3.8,<4.0',
}


setup(**setup_kwargs)
