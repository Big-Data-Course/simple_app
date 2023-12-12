from setuptools import setup, find_packages

setup(
    name="application",
    version="0.1.0",
    packages=find_packages(include=["application", "application.*"]),
    install_requires=[
        "astropy==5.3.4",
        "astroquery==0.4.6",
        "numpy==1.26.0",
        "mpl-scatter-density==0.7",
        "matplotlib==3.8.0",
        "pandas==2.1.3"
    ],
    entry_points={
        'console_scripts': ['gaia_analyze=application.app:main']
    }
)