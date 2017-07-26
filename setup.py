from setuptools import setup

setup(name='ndasynapse',
      version='0.1',
      description='NDA to Synapse sync',
      url='http://github.com/bsmn/ndasynapse',
      author='Kenneth Daily',
      author_email='kenneth.daily@sagebase.org',
      license='MIT',
      packages=['ndasynapse'],
      scripts=['bin/nda_to_synapse_manifest.py'],
      zip_safe=False)
