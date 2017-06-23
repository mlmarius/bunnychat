from setuptools import setup

setup(name='bunnychat',
      version='0.1',
      description='Handle comunication with a RabbitMQ server',
      maintainer='Liviu Manea',
      maintainer_email='mlmarius@yahoo.com',
      packages=['bunnychat'],
      license='BSD',
      install_requires=[
          # 'pika=0.10.0',
          'pika'
      ],
      dependency_links=[
          # 'git+ssh://git@github.com/mlmarius/pika.git@64f8f12#egg=pika-0.10.0',
          'git+ssh://git@github.com/mlmarius/pika.git@64f8f12#egg=pika'
      ],
      package_data={'': ['LICENSE', 'README.rst']},
      classifiers=[],
      zip_safe=True)
