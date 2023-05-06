from setuptools import setup

setup(
    name="bp-query-builder",
    version="0.1",
    description="A simple query builder for Python written with ChatGPT4",
    author="Antonio Gallo",
    author_email="agx@linux.it",
    packages=["."],
    install_requires=["sqlite3", "pymysql", "psycopg2"],
)
