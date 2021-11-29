import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="emr_fs",
    version="0.0.1",
    author="Jeff Tang",
    author_email="tangqy@amazon.com",
    description="emr offline featurestore for sagemaker package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/qingyuan18/emr_fs",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)
