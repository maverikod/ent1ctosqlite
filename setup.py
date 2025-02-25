from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ent1ctosqlite",
    version="0.1.1",
    author="maverikod",
    author_email="vasilyvz@gmail.com",
    description="A tool for converting 1C:Enterprise configurations to SQLite database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/maverikod/ent1ctosqlite",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        'setuptools',
    ],
    extras_require={
        'dev': [
            'pytest>=6.0',
            'pytest-cov',
            'black',
            'flake8',
            'build',
            'twine',
        ],
    },
    entry_points={
        'console_scripts': [
            'ent1ctosqlite=ent1ctosqlite.cli:main',
        ],
    },
)
