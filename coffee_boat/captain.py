import inspect
import pyspark
import sys
import subprocess
import shutil
import tempfile
import uuid


__all__ = ['Captain']

handle_del = False

try:
    from subprocess import DEVNULL  # py3k
except ImportError:
    import os
    DEVNULL = open(os.devnull, 'wb')


class Captain(object):
    """
    The captain of the coffee boat is used to setup packages for Spark to use.
    To use it run init, call the `add_pip_packages` for whatever you wish to
    add and then `launch_ship` before you create your SparkContext.

    The coffee boat captain currently works by creating a conda env and
    shipping it.

    Create a captain to captain the coffee boat and install the packages.

        Currently only supports conda, TODO:PEX for others.
        :param use_conda: Build a conda package rather than a pex package.
        :param install_local: Attempt to install packages locally as well
        :param env_name: Environment name to use. May squash existing
            environment
        :param working_dir: Directory for working in.
        :param accept_conda_license: If you accept the conda license. Set it
            True to work.
        :param conda_path: Path to conda (optional). Otherwise searches system
            or self-installs.

    """

    def __init__(self,
                 use_conda=True,
                 install_local=True,
                 env_name=None,
                 working_dir=None,
                 accept_conda_license=False,
                 python_version=None,
                 conda_path=None):
        self.accept_conda_license = accept_conda_license
        self.working_dir = working_dir
        self.install_local = install_local
        self.env_name = env_name or "auto{0}".format(str(uuid.uuid4()))
        # Kind of hackey, but yay shells....
        self.env_name = self.env_name.replace('-', "_")
        self.python_version = python_version or '.'.join(map(str, sys.version_info[:3]))
        if not self.working_dir:
            self.working_dir = tempfile.mkdtemp(prefix="coffee_boat_tmp_")
            import atexit
            if handle_del:
                atexit.register(lambda: shutil.rmtree(self.working_dir))
        self.use_conda = use_conda
        self.conda = conda_path
        self.pip_pkgs = []
        return

    def add_pip_packages(self, *pkgs):
        """Add pip packages"""
        self._raise_if_running()
        if self.install_local:
            args = ["pip", "install"]
            args.extend(pkgs)
            subprocess.check_call(args, stdout=DEVNULL)
        self.pip_pkgs.extend(pkgs)

    def launch_ship(self):
        """Creates a relocatable environment and distributes it.

        .. note::

           This function must be called before you init your SparkContext!

        """
        self._raise_if_running()
        # Doing sketchy things with the gateway
        if pyspark.context.SparkContext._gateway is not None:
            try:
                pyspark.context.SparkContext._gateway.jvm.java.lang.System.exit(0)
            except Exception:
                pass
            self._cleanup_keys()
            pyspark.context.SparkContext._gateway = None
        # Dispatch
        if self.use_conda:
            self._setup_or_find_conda()
            return self._launch_conda_ship()
        else:
            # TODO pex implementation
            return self._launch_pex()

    def _launch_conda_ship(self):
        """ Create a conda environment, zips it up, and manipulate the
        environment variables.

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
        package_spec_file = tempfile.NamedTemporaryFile(dir=self.working_dir,
                                                        delete=handle_del)
        package_spec_path = package_spec_file.name
        print("Writing package spec to {0}.".format(package_spec_path))
        package_spec_file.write(package_spec)
        package_spec_file.flush()

        # Create the conda env
        conda_prefix = os.path.join(self.working_dir, self.env_name)
        print("Creating conda env")
        if os.path.exists(conda_prefix):
            print("Cleaning up old prefix {0}".format(conda_prefix))
            subprocess.check_call(["rm", "-rf", conda_prefix])
        subprocess.check_call([self.conda, "env", "create",
                               "-f", package_spec_path,
                               "--prefix", conda_prefix],
                              stdout=DEVNULL)

        # Package it for distro
        zip_name = "coffee_boat_{0}.zip".format(self.env_name)
        zip_target = os.path.join(self.working_dir, zip_name)
        print("Packaging conda env")
        subprocess.check_call(["zip", zip_target, "-r", conda_prefix], stdout=DEVNULL)
        relative_python_path = "." + conda_prefix + "/bin/python"

        # Make a self extractor script
        runner_script = inspect.cleandoc("""#!/bin/bash
        if [ -f {0} ];
        then
          unzip {0} &>/dev/null && rm {0} &> /dev/null
        fi
        {1} "$@" """.format(zip_name, relative_python_path))
        script_name = "coffee_boat_runner_{0}.sh".format(self.env_name)
        runner_script_path = os.path.join(self.working_dir, script_name)
        with open(runner_script_path, 'w') as f:
            f.write(runner_script)
        subprocess.check_call(["chmod", "a+x", runner_script_path])

        # Screw around with environment variables so that the env gets
        # distributed.
        old_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        # Backup the old arguments
        if "coffee_boat" not in old_args:
            os.environ["BACK_PYSPARK_SUBMIT_ARGS"] = old_args
        else:
            old_args = os.environ.get("BACK_PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        new_args = "--files {0},{1} {2}".format(zip_target, runner_script_path, old_args)
        print("using {0} as python arguments".format(new_args))
        os.environ["PYSPARK_SUBMIT_ARGS"] = new_args
        if "PYSPARK_GATEWAY_PORT" in os.environ:
            print("Hey the Java process is already running, this might not work.")
        os.environ["PYSPARK_PYTHON"] = "./{0}".format(script_name)

    def _launch_pex(self):
        """ Create a pex environment."""
        pass

    def _raise_if_running(self):
        """Raise an exception if Spark is already running because it's too
        late to add or package dependencies at this point.

        """
        if pyspark.SparkContext._active_spark_context is not None:
            raise Exception(
                "Spark context is already running. "
                "All coffee boat activities must occur before launching your SparkContext."
                "You can stop your current SparkContext (with sc.stop() or session.stop()) "
                "add more dependencies and re-start your SparkContext if needed.")

    def _setup_or_find_conda(self):
        """Find conda or set up a conda installation"""
        # Check if we need to setup conda or return if we already have one
        rc = subprocess.call(['which', 'conda'])
        if rc == 0:
            self.conda = "conda"
            return
        if self.conda is not None:
            return

        # Install conda if we need to
        if not self.accept_conda_license:
            raise Exception("Please accept the conda license by setting "
                            "accept_conda_license")
        python_version = sys.version_info[0]
        url = "https://repo.continuum.io/miniconda/Miniconda%d-latest-Linux-x86_64.sh" % python_version
        print("Downloading conda from %s to %s" % (url, self.working_dir))
        mini_conda_target = "%s/%s" % (self.working_dir, "miniconda.sh")
        subprocess.check_call(["wget", url, "-O", mini_conda_target, "-nv"],
                              shell=False,
                              stdout=DEVNULL)
        print("Running conda setup....")
        subprocess.check_call(["chmod", "a+x", mini_conda_target],
                              shell=False,
                              stdout=DEVNULL)
        conda_target = "%s/%s" % (self.working_dir, "conda")
        subprocess.check_call([mini_conda_target, "-b", "-p", conda_target],
                              stdout=DEVNULL,
                              stderr=DEVNULL)
        self.conda = "%s/bin/conda" % conda_target
