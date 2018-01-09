import inspect
import pyspark
import sys
import subprocess
import shutil
import tempfile
import os


__all__ = ['Captain']

handle_del = False

try:
    from subprocess import DEVNULL # py3k
except ImportError:
    import os
    DEVNULL = open(os.devnull, 'wb')

class Captain():
  """
  The captain of the coffee boat is used to setup packages for Spark to use.
  To use it run init, call the `add_pip_packages` for whatever you wish to add and then
  `launch_ship` before you create your SparkContext.

  The coffee boat captain currently works by creating a conda env and shipping it.
  """

  def __init__(self,
               use_conda=True,
               install_local=True,
               env_name=None,
               working_dir=None,
               accept_conda_license=False,
               python_version=None):
    """Create a captain to captain the coffee boat and install the packages.
    Currently only supports conda, TODO:PEX for others.
    :param use_conda: Build a conda package rather than a pex package.
    :param install_local: Attempt to install packages locally as well
    :param env_name: Enviroment name to use. May squash existing enviroment
    :param working_dir: Directory for working in.
    :param accept_conda_license: If you accept the conda license. Set it True to work.
    """
    self.accept_conda_license = accept_conda_license
    self.working_dir = working_dir
    self.install_local = install_local
    self.env_name = env_name or "coffee_boat"
    self.python_version = python_version or '.'.join(map(str, sys.version_info[:3]))
    if not self.working_dir:
      self.working_dir = tempfile.mkdtemp(prefix="coffee_boat_tmp_")
      import atexit
      if handle_del:
        atexit.register(lambda: shutil.rmtree(self.working_dir))
    self.use_conda = use_conda
    if use_conda:
      self._setup_or_find_conda()
    self.pip_pkgs = []
    return

  def add_pip_packages(self, *pkgs):
    """Add a pip packages"""
    self._raise_if_running()
    if self.install_local:
      args = ["pip", "install"]
      args.extend(pkgs)
      subprocess.check_call(args, stdout=DEVNULL)
    self.pip_pkgs.extend(pkgs)

  def launch_ship(self):
    """Creates a relocatable enviroment and distributes it.
    .. note::
       This function must be called before your init your SparkContext!"""
    self._raise_if_running()
    if self.use_conda:
      return self._launch_conda_ship()
    else:
      return self._launch_pex()

  def _launch_conda_ship(self):
    """Create a conda enviroment, zips it up, and manipulate the enviroment variables.
    """
    # Create the conda package env spec
    pkgs = [""]
    pkgs.extend(map(str, self.pip_pkgs))
    pip_packages = '\n  - '.join(pkgs)
    # Create the package_spec
    base_package_spec = inspect.cleandoc("""
    name: {0}
    dependencies:
    - python=={1}
    - anaconda
    - pip
    - pip:
    """).format(self.env_name, self.python_version)
    package_spec = "{0}{1}".format(base_package_spec, pip_packages)
    package_spec_file = tempfile.NamedTemporaryFile(dir=self.working_dir, delete=handle_del)
    package_spec_path = package_spec_file.name
    print("Writing package spec to {0}.".format(package_spec_path))
    package_spec_file.write(package_spec)
    package_spec_file.flush()
    # Create the conda env
    conda_prefix = os.path.join(self.working_dir, self.env_name)
    print("Creating conda env")
    if os.path.exists(conda_prefix):
      print("Cleaining up old prefix {0}".format(conda_prefix))
      subprocess.check_call(["rm", "-rf", conda_prefix])
    subprocess.check_call([self.conda, "env", "create", "-f", package_spec_path,
                           "--prefix", conda_prefix], stdout=DEVNULL)
    # Package it for distro
    zip_target = os.path.join(self.working_dir, "coffee_boat.zip")
    print("Packaging conda env")
    subprocess.check_call(["zip", zip_target, "-r", conda_prefix], stdout=DEVNULL)
    relative_python_path = "." + conda_prefix + "/bin/python"

    # Make a self extractor script
    runner_script = inspect.cleandoc("""#!/bin/bash
    if [ -f coffee_boat.zip ];
    then
      unzip coffee_boat.zip && rm coffee_boat.zip
    fi
    {0}""".format(relative_python_path))
    runner_script_path = os.path.join(self.working_dir, "coffee_boat_runner.sh")
    with open(runner_script_path, 'w') as f:
      f.write(runner_script)
    subprocess.check_call(["chmod", "a+x", runner_script_path])

    # Screw around with enviroment variables so that the env gets distributed.
    old_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
    new_args = "--py-files {0},{1} {2}".format(zip_target, runner_script_path, old_args)
    os.environ["PYSPARK_SUBMIT_ARGS"] = new_args
    if "PYSPARK_GATEWAY_PORT" in os.environ:
      print("Hey the Java process is already running, this might not work.")
    os.environ["PYSPARK_PYTHON"] = "./coffee_boat_runner.sh"



  def _raise_if_running(self):
    """Raise an exception if Spark is already running because its too late to add or package
    dependencies at this point."""
    if pyspark.SparkContext._active_spark_context is not None:
      raise Exception("Spark context is already running. "
                      "All coffee boat activites must occure before launching your SparkContext."
                      "You can stop your current SparkContext (with sc.stop() or session.stop()) "
                      "add more dependencies and re-start your SparkContext if needed.")


  def _setup_or_find_conda(self):
    # Check if we need to setup conda or return if we already have one
    rc = subprocess.call(['which', 'conda'])
    if rc == 0:
      self.conda = "conda"
      return
    # Install conda if we need to
    if not self.accept_conda_license:
      raise Exception("Please accept the conda license by setting accept_conda_license")
    python_version = sys.version_info[0]
    url = "https://repo.continuum.io/miniconda/Miniconda%d-latest-Linux-x86_64.sh" % python_version
    print("Downloading conda from %s to %s" % (url, self.working_dir))
    mini_conda_target = "%s/%s" % (self.working_dir, "miniconda.sh")
    subprocess.check_call(["wget", url, "-O", mini_conda_target, "-nv"], shell=False,
                          stdout=DEVNULL)
    print("Running conda setup....")
    subprocess.check_call(["chmod", "a+x", mini_conda_target], shell=False, stdout=DEVNULL)
    conda_target = "%s/%s" % (self.working_dir, "conda")
    subprocess.check_call([mini_conda_target, "-b", "-p", conda_target], stdout=DEVNULL, stderr=DEVNULL)
    self.conda = "%s/bin/conda" % conda_target
