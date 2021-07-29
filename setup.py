from setuptools import setup

setup(
    name='mongo-apify',
    version='0.1.0',    
    description='A example Python package',
    url='https://github.com/shuds13/pyexample',
    author='Stephen Hudson',
    author_email='shudson@anl.gov',    
    license='BSD 2-clause',
    packages=['mongoapify'],
    # install_requires=['mpi4py>=2.0',
    #                   'numpy',
    #                   ],

    classifiers=[
        'Development Status :: 1 - Yolo',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',  
        'Operating System :: POSIX :: Linux',        
        'Programming Language :: Python :: 3',
    ],
)    
