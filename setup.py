from setuptools import setup, find_packages

setup(
    name="panama_datos_abiertos",
    version="0.0.1",
    description="Codigo para descargar archivos/datos publicos del gobierno de Panama",
    packages=find_packages(where="./src"),
    package_dir={"": "./src"},
    install_requires=["openpyxl"],
)
