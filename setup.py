# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['exasol_advanced_analytics_framework',
 'exasol_advanced_analytics_framework.deployment',
 'exasol_advanced_analytics_framework.query_handler',
 'exasol_advanced_analytics_framework.query_handler.context',
 'exasol_advanced_analytics_framework.query_handler.context.proxy',
 'exasol_advanced_analytics_framework.query_handler.query',
 'exasol_advanced_analytics_framework.query_result',
 'exasol_advanced_analytics_framework.testing',
 'exasol_advanced_analytics_framework.udf_communication',
 'exasol_advanced_analytics_framework.udf_framework']

package_data = \
{'': ['*'],
 'exasol_advanced_analytics_framework': ['lua/src/*',
                                         'lua/test/*',
                                         'resources/outputs/*',
                                         'resources/templates/*']}

install_requires = \
['Jinja2>=3.0.3,<4.0.0',
 'click>=8.0.4,<9.0.0',
 'exasol-bucketfs-utils-python @ '
 'git+https://github.com/exasol/bucketfs-utils-python.git@main',
 'exasol-data-science-utils-python @ '
 'git+https://github.com/exasol/data-science-utils-python.git@main',
 'importlib-resources>=5.4.0,<6.0.0',
 'pandas>=1.1.0,<2.0.0',
 'pydantic>=1.10.2,<2.0.0',
 'pyexasol>=0.25.0,<0.26.0',
 'pyzmq>=24.0.1,<25.0.0']

setup_kwargs = {
    'name': 'exasol-advanced-analytics-framework',
    'version': '0.1.0',
    'description': 'Framework for building complex data analysis algorithms with Exasol',
    'long_description': '# Exasol Advanced Analytics Framework\n\n**This project is at an early development stage.**\n\nFramework for building complex data analysis algorithms with Exasol.\n\n\n## Information for Users\n\n- [User Guide](doc/user_guide/user_guide.md)\n- [System Requirements](doc/system_requirements.md)\n- [Design](doc/design.md)\n- [License](LICENSE)\n\n## Information for Developers\n\n- [Developers Guide](doc/developer_guide/developer_guide.md)\n\n',
    'author': 'Umit Buyuksahin',
    'author_email': 'umit.buyuksahin@exasol.com',
    'maintainer': 'None',
    'maintainer_email': 'None',
    'url': 'https://github.com/exasol/advanced-analytics-framework',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.8,<4.0',
}


setup(**setup_kwargs)
