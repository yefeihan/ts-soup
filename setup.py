import setuptools

with open("README.md", "r",encoding='utf8') as fh:
  long_description = fh.read()

setuptools.setup(
  name="ts-soup",
  version="0.0.6",
  author="feihan ye",
  author_email="445280206@qq.com",
  description="date series data synchronization",
  long_description=long_description,
  long_description_content_type="text/markdown",
  # url="https://github.com/pypa/sampleproject",
  packages=setuptools.find_packages(),
  classifiers=[
  "Programming Language :: Python :: 3",
  "License :: OSI Approved :: MIT License",
  "Operating System :: OS Independent",
  ],
)