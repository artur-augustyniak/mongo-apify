from setuptools import setup

setup(
    name='mongo-apify',
    version='0.1.0',    
    license='BSD 2-clause',
    packages=['mongoapify'],
    install_requires=['pymongo==3.11.4',
                      'python-logstash==0.4.6',
                      'logaugment==0.1.3',
                      'dnspython==2.1.0',
                      'gunicorn==20.1.0',
                      'flask-cors==3.0.10',
                      'connexion[swagger-ui]==2.8.0',
                      ],

    classifiers=[
        'Development Status :: 1 - Yolo',
        'License :: OSI Approved :: BSD License',  
        'Programming Language :: Python :: 3',
    ],
)    
