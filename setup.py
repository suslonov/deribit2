import os
import shutil
from pathlib import Path
from setup_helpers import wheel_name
from setuptools import find_packages, setup


setup_kwargs = dict(
    name='deribit2',
    version="0.0.1",
    license='LICENSE',
    platforms='All',
    description='fr simple deribit library',
    packages=find_packages("."),
    include_package_data=True,
    install_requires=[
        "pytz",
        "pandas",
        "wheel",
	"lib2",
        "mysqlclient",
    ],
    python_requires='>=3.5'
)

# get the wheel name
wheel_filename = wheel_name(**setup_kwargs)

# build package
root = Path(__file__).parent
os.chdir(str(root))
setup(**setup_kwargs)

# create latest wheel
version = setup_kwargs['version']
wheel_latest_filename = wheel_filename.replace(version, "latest")
wheels_dir = Path("wheel")
wheels_dir.mkdir(parents=True, exist_ok=True)
generated_wheel = Path("dist").joinpath(wheel_filename)
if generated_wheel.exists():
    shutil.copy(Path("dist").joinpath(wheel_filename), wheels_dir.joinpath(wheel_latest_filename))
