from setuptools import find_packages 
from setuptools import setup 

setup( 
    name='Coding-Challenge', 
    version='1.0', 
    install_requires=[ 
        'apache-beam[gcp]==2.50.0', 
        'geopy==1.18.0', 
    ], 
    packages=find_packages(exclude=['notebooks']), 
    py_modules=['config'], 
    include_package_data=True, 
    description='Coding Challenge' 
)
