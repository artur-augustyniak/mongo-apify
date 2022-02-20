from setuptools import setup

setup(
    name='mongoapify',
    version='0.1.0',    
    license='BSD 2-clause',
    packages=['mongoapify'],
    install_requires=['pymongo==3.11.4',
                      'dnspython==2.1.0',
                      'gunicorn==20.1.0',
                      'flask-cors==3.0.10',
                      'connexion[swagger-ui]==2.11.2'
                      ],

    classifiers=[
        'Development Status :: 1 - Yolo',
        'License :: OSI Approved :: BSD License',  
        'Programming Language :: Python :: 3',
    ],
)    
