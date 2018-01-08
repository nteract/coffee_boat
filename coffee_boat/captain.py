import pyspark
import sys
import subprocess
import tempfile

__all__ = ['Captain']

class Captain():
  """
  The captain of the coffee boat is used to setup packages for Spark to use.
  To use it run init, call the `add_pip_packages` for whatever you wish to add and then
  `launch_ship` before you create your SparkContext.
  """

  def __init__(self, base_env="anaconda", install_local=True, env_name=None,
               working_dir=None,
               accept_conda_license=False):
    """Create a captain to captain the coffee boat and install the packages.
    :param base_env: Base enviroment to use. Only supported in conda.
    :param install_local: Attempt to install packages locally as well
    :param env_name: Enviroment name to use. May squash existing enviroment
    :param working_dir: Directory for working in.
    :param accept_conda_license: If you accept the conda license. Set it True to work.
    """
    self.working_dir = working_dir
    if not self.working_dir:
      self.working_dir = tempfile.mkdtemp()
    self._setup_or_find_conda()
    self.base_env = base_env
    self.pip_pkgs = []
    return

  def add_pip_packages(self, *pkgs):
    """Add a pip packages"""
    self._raise_if_running()
    if install_local:
      args = ["pip", "install"]
      args.extend(pkgs)
      subprocess.check_call(args)
    self.pip_pkgs.extend(pkgs)

  def launch_ship(self):
    """Create a conda enviroment, tar it up, and manipulate the enviroment variables.
    .. note::
       This function must be called before your init your SparkContext!
    """
    self._raise_if_running()


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
    python_version = sys.version_info[0]
    url = "https://repo.continuum.io/miniconda/Miniconda%d-latest-Linux-x86_64.sh" % python_version
    print("Downloading conda from %s to %s" % (url, self.working_dir))
    mini_conda_target = "%s/%s" % (self.working_dir, "miniconda.sh")
    subprocess.check_call(["wget", url, "-O", mini_conda_target, "-nv"], shell=False)
    print("Running conda setup....")
    subprocess.check_call(["chmod", "a+x", mini_conda_target], shell=False)
    conda_target = "%s/%s" % (self.working_dir, "conda")
    subprocess.check_call(["miniconda.sh", "-b", "-p", conda_target])
    self.conda = "%s/bin/conda" % conda_target
