import setuptools

setuptools.setup(
    name="pyieamods",
    version="1.0.8",
    author="aeorxc",
    author_email="author@example.com",
    description="wrapper around IEA Monthly Oil Data Service",
    url="https://github.com/aeorxc/pyieamods",
    project_urls={
        'Source': 'https://github.com/aeorxc/pyieamods',
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['pandas', 'dask[delayed]'],
    python_requires='>=3.6',
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)

