from setuptools import setup, find_packages

setup(
    name="hubspot_export_api",
    version="0.1.0",
    description="A Python package for exporting HubSpot data, processing it, and uploading to AWS S3.",
    author="Jing Su",
    packages=find_packages(), 
    install_requires=[
        "requests",
        "pandas",
        "boto3",
        "pyarrow",
        "python-dotenv"
    ],
    python_requires=">=3.7",
)
