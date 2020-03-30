import os
from setuptools import setup

# determine the version
about = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "ndasynapse", "__version__.py")) as f:
    exec(f.read(), about)

setup(name='ndasynapse',
      version=about['__version__'],
      description='NDA to Synapse sync',
      url='http://github.com/bsmn/ndasynapse',
      author='Kenneth Daily',
      author_email='kenneth.daily@sagebase.org',
      license='MIT',
      packages=['ndasynapse'],
      setup_requires=['numpy>=1.13.1'],
      tests_requires=["pytest", "nose"],
      install_requires=['synapseclient>=1.7.2',
                        'pandas>=0.20.3',
                        # TODO: revert this once https://github.com/boto/botocore/issues/1872 is fixed.
                        # 'boto3>=1.4.2',
                        'boto3==1.10.6',
                        'botocore==1.13.9',
                        'boto>=2.46.1',
                        'requests>=2.18.1',
                        'deprecated==1.2.4'],
      scripts=['bin/nda_to_synapse_manifest.py', 'bin/manifest_to_synapse.py', 'bin/query-nda', 'bin/manifest_guid_data.py'],
      zip_safe=False)
