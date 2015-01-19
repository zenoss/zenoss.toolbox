from setuptools import setup, find_packages
import os

here = lambda *x:os.path.join(os.path.dirname(__file__), *x)

version = '0.5.2'

long_description = (
    open(here('README.rst')).read()
    + '\n' +
    open(here('CHANGES.txt')).read()
    + '\n')

setup(name='zenoss.toolbox',
      version=version,
      description="Utilities for analyzing and debugging Zenoss environments.",
      long_description=long_description,
      # Get more strings from
      # http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
        "Programming Language :: Python",
        ],
      keywords='',
      author='Zenoss, Inc.',
      author_email='dev@zenoss.com',
      url='git@github.com:zenoss/zenoss.toolbox.git',
      license='Proprietary',
      packages=find_packages('src'),
      package_dir = {'': 'src'},
      namespace_packages=['zenoss'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools',
      ],
      entry_points=open('scripts.conf').read(),
      )
