from setuptools import find_packages, setup

setup(
    name="kerchunk_gefsv12_retro",  # Package name
    version="0.1",  # Package version
    packages=find_packages(),  # Automatically find subpackages
    install_requires=[""],  # Dependencies (optional)
    author="Taylor Mandelbaum",  # Package author
    author_email="mandelbaum.taylor@gmail.com",  # Author email
    description="Speedy access to the GEFSv12 Retrospective dataset",  # Short description
    long_description=open("README.md").read(),  # Optional long description from README
    long_description_content_type="text/markdown",  # README format
    url="https://github.com/aaTman/kerchunk_gefsv12_retro",  # Optional URL (GitHub repo)
    classifiers=[  # Classifiers (optional metadata)
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",  # Minimum Python version
)
