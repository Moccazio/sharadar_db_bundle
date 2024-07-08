from setuptools import setup

setup(
    name='sharadar_db_bundle',
    version='2.0',
    packages=['sharadar', 'sharadar.data', 'sharadar.loaders', 'sharadar.pipeline', 'sharadar.util', 'sharadar.statistic'
              , 'sharadar.live', 'sharadar.live.brokers' ],
    url='https://github.com/Moccazio/sharadar_db_bundle',
    license='',
    author='',
    author_email='',
    description='custom sharadar data bundle for zipline-reloaded',
    entry_points = {
                   'console_scripts': [
                       'sharadar-zipline = sharadar.__main__:main',
                   ],
               }
)
