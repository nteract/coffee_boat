import inspect
import pyspark
import sys
import subprocess
import shutil
import tempfile
import uuid
import warnings
import os

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

    """
    def __init__(self,
                 use_conda=True,
                 install_local=True,
                 env_name=None,
                 working_dir=None,
                 accept_conda_license=False,
                 enable_live_install=True,
                 python_version=None,
                 conda_path=None):
        """Create a captain to captain the coffee boat and install the packages.

        Currently only supports conda, TODO:PEX for others.

        :param use_conda: Build a conda package rather than a pex package.
        :param install_local: Attempt to install packages locally as well
        :param env_name: Enviroment name to use. May squash existing enviroment
        :param working_dir: Directory for working in.
        :param accept_conda_license: If you accept the conda license. Set it
            to True to work.
        :param enable_live_install: Install at runtime between coffee boat launches.
        :param conda_path: Path to conda (optional). Otherwise searches system
            or self-installs.

        """
        self.accept_conda_license = accept_conda_license
        self.working_dir = working_dir
        self.install_local = install_local
        self.env_name = env_name or "auto{0}".format(str(uuid.uuid4()))
        # Kind of hackey, but yay shells & yay conda.
        self.env_name = self.env_name.replace('-', "")
        self.python_version = (python_version or
                               '.'.join(map(str, sys.version_info[:3])))
        self.enable_live_install = enable_live_install
        if not self.working_dir:
            self.working_dir = tempfile.mkdtemp(prefix="coffee_boat_tmp_")
            import atexit
            if handle_del:
                atexit.register(lambda: shutil.rmtree(self.working_dir))
        self.use_conda = use_conda
        self.conda = conda_path
        self.remote_conda = None
        self.pip_pkgs = []
        return

    def add_pip_packages(self, *pkgs):
        """Add pip packages"""
        sc = pyspark.context.SparkContext._active_spark_context

        if sc is not None:
            if os.environ.get("COFFEE_BACK_PYSPARK_SUBMIT_ARGS") is None:
                print("Adding pip package. Remember to launch your coffee boat before trying to use!")
            elif self.enable_live_install:
                print("You've already launched your coffee boat. I'll add this package at runtime"
                      " using magic, but next time add your packages before so I don't have to use"
                      " my magical super powers (aka make your code slow).")
                # Step 1: Setup a dist file so any new hosts coming online will install
                # TODO: Test this better (probably with circle CI)
                pip_req_file = tempfile.NamedTemporaryFile(
                    dir=self.working_dir, delete=handle_del, prefix="magicCoffeeReq")
                pip_req_path = pip_req_file.name
                print("Writing req file to {0}.".format(pip_req_path))
                pip_req_file.write("\n".join(pkgs))
                pip_req_file.flush()
                sc.addFile(pip_req_path)
                # Step 2: install the package on the running hosts
                memory_status_count = sc._jsc.sc().getExecutorMemoryStatus().size()
                # TODO: This is kind of a hack. Figure out if its dangerous (aka wrong)
                estimated_executors = max(sc.defaultParallelism, memory_status_count)
                print("Estimated number of executors is {0}".format(estimated_executors))
                rdd = sc.parallelize(range(estimated_executors))

                my_pkgs = pkgs

                def install_remote(x):
                    import subprocess
                    import sys
                    print("Installing on executor {0} with python {1}".format(x, sys.executable))
                    args = ["pip", "install"]
                    args.extend(my_pkgs)
                    print("Args for install are: {0}".format(args))
                    subprocess.call(args)
                    return 1

                rdd.foreach(install_remote)
                print("Installed package on remote")
            else:
                print("Spark context already running, remote install disabled."
                      "re-launch of coffee boat required to provide package.")
        else:
            print("Adding package to requirements. Remember to launch coffee boat before"
                  "starting SparkContext.")

        if self.install_local:
            print("Installing package locally")
            import subprocess
            args = ["pip", "install"]
            args.extend(pkgs)
            subprocess.call(args, stdout=DEVNULL)
        self.pip_pkgs.extend(pkgs)

    def launch_ship(self):
        """Creates a relocatable environment and distributes it.

        .. note::

           This function *should* be called before you init your SparkContext, if it's
           called after we need to do some sketchy things to make it work.
        """
        # Doing sketchy things with the gateway if we've already stopped the context
        active_context = pyspark.context.SparkContext._active_spark_context
        gateway = pyspark.context.SparkContext._gateway
        if active_context is None and gateway is not None:
            try:
                pyspark.context.SparkContext._gateway.jvm.java.lang.System.exit(0)
            except Exception:
                pass
            self._cleanup_keys()
            pyspark.context.SparkContext._gateway = None
        elif active_context is not None:
          warnings.warn(
              "Launching on an existing SparkContext. Packages will only be available to RDDs"
              "created from here forward. If this makes you sad, stop the Spark context and"
              "re-create those RDDs you want to have access to your packages in.")


        if self.use_conda:
            self._setup_or_find_conda()
            return self._launch_conda_ship()
        else:
            return self._launch_pex()

    def _launch_conda_ship(self):
        """Create a conda enviroment, zips it up, and manipulate the environment
        variables.

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
            print("Cleaining up old prefix {0}".format(conda_prefix))
            subprocess.check_call(["rm", "-rf", conda_prefix])
        subprocess.check_call([self.conda, "env", "create",
                               "-f", package_spec_path,
                               "--prefix", conda_prefix],
                              stdout=DEVNULL)

        # Package it for distro
        zip_name = "coffee_boat_{0}.zip".format(self.env_name)
        zip_target = os.path.join(self.working_dir, zip_name)
        print("Packaging conda env")
        subprocess.check_call(["zip", zip_target, "-r", conda_prefix],
                              stdout=DEVNULL)
        relative_conda_path = ".{0}".format(conda_prefix)
        self.remote_conda = relative_conda_path

        # Make a self extractor script
        runner_script = inspect.cleandoc("""#!/bin/bash
        touch setup.lock # TODO: work with this
        echo "Kicking off python runner {0}" > coffee_log.txt
        echo "pwd looks like:" >> coffee_log.txt
        ls >> coffee_log.txt
        echo "k" >> coffee_log.txt
        if [ -f {0} ];
        then
            echo "Running setup" >> coffee_log.txt
            unzip {0} &>> coffee_log.txt && rm {0} &>> coffee_log.txt
            # Since Conda isn't really fully relocatable...
            echo "Rewriting conda and pip python paths..." >> coffee_log.txt
            sed -i -e "1s@.*@\#\!{1}/bin/python@" {1}/bin/conda >> coffee_log.txt
            sed -i -e "1s@.*@\#\!{1}/bin/python@" {1}/bin/pip >> coffee_log.txt
        fi
        cat magicCoffeeReq* > mini_req.txt &> /dev/null || true
        echo "pip install" >> coffee_log.txt
        {1}/bin/pip install -r mini_req.txt &>> coffee_log.txt
        export PATH={1}/bin:$PATH
        {1}/bin/python "$@" || cat coffee_log.txt 1>&2 """.format(zip_name, relative_conda_path))
        print("Using runner script\n{0}".format(runner_script))
        script_name = "coffee_boat_runner_{0}.sh".format(self.env_name)
        runner_script_path = os.path.join(self.working_dir, script_name)
        with open(runner_script_path, 'w') as f:
            f.write(runner_script)
        subprocess.check_call(["chmod", "a+x", runner_script_path])

        # Adjust environment variables so that the env gets distributed.
        old_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        # Backup the old arguments
        if "coffee_boat" not in old_args:
            os.environ["COFFEE_BACK_PYSPARK_SUBMIT_ARGS"] = old_args
        else:
            old_args = os.environ.get("COFFEE_BACK_PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        new_args = "--files {0},{1} {2}".format(zip_target, runner_script_path, old_args)
        print("using {0} as python arguments".format(new_args))
        os.environ["PYSPARK_SUBMIT_ARGS"] = new_args
        # Handle active/already running contexts.
        sc = pyspark.context.SparkContext._active_spark_context
        if sc is not None:
            print("Adding {0} & {1} to existing sc".format(zip_target, runner_script_path))
            sc.addFile(zip_target)
            sc.addFile(runner_script_path)
            print("Updating python exec on existing sc")
            sc.pythonExec = "./{0}".format(script_name)
        else:
            print("No active context, depending on submit args.")

        if "PYSPARK_GATEWAY_PORT" in os.environ:
            print("Hey the Java process is already running, this might not work.")
        os.environ["PYSPARK_PYTHON"] = "./{0}".format(script_name)

    def _launch_pex(self):
        """ Create a pex environment."""
        pass

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

    def _cleanup_keys(self):
        import os
        def cleanup_key(name):
            if name in os.environ:
                del os.environ[name]
        keys = [
            "PYSPARK_PYTHON",
            "PYSPARK_GATEWAY_PORT",
            "_PYSPARK_DRIVER_CALLBACK_HOST",
            "_PYSPARK_DRIVER_CALLBACK_PORT"]
        map(cleanup_key, keys)
