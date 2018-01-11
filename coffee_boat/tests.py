# Basics tests for coffee boat. Note requires running local cluster.
import pyspark
import unittest2


class TestBasicDep(unittest2.TestCase):
    """Test we package dependencies. Note: this test case is EVIL!"""
    _multiprocess_can_split_ = False

    def test_simple_env(self):
        import os
        print(os.environ)
        from coffee_boat import Captain
        # Create a captain
        captain = Captain(accept_conda_license=True)

        # Validate we don't have nbconvert installed in local context
        import subprocess
        subprocess.call(["pip", "uninstall", "-y", "nbconvert"])
        with self.assertRaises(ImportError):
            import nbconvert
        captain.add_pip_packages("pandas==0.22.0", "nbconvert")

        # We should now have it....
        import nbconvert
        import pandas
        captain.launch_ship()
        print("Ship launched , set PYSPARK_PYTHON to {0}".format(os.environ['PYSPARK_PYTHON']))
        print("Connecting to local Spark master.")
        sc = pyspark.context.SparkContext(master="spark://localhost:7077")
        try:
            rdd = sc.parallelize(range(2))
            def find_info(x):
                import sys
                import os
                return (x,
                        sys.executable,
                        os.environ['PYTHONPATH'],
                        os.listdir('.'))
            result = rdd.map(find_info).collect()
            print("Result {0}".format(result))
            def test_imports(x):
                import nbconvert
                return 1
            rdd.map(test_imports).collect()
        finally:
            sc.stop()
        self.assertTrue("auto" in result[0][1])
        self.assertTrue("python" in result[0][1])

    # TODO: figure out why we need a new python process.
    def test_non_local_env(self):
        import os
        print(os.environ)
        from coffee_boat import Captain
        # Create a captain
        captain = Captain(install_local=False, accept_conda_license=True)
        # Validate we don't have kubepy installed in local context
        import subprocess
        subprocess.call(["pip", "uninstall", "-y", "kubepy"])
        with self.assertRaises(ImportError):
            import kubepy
        captain.add_pip_packages("pandas==0.22.0", "kubepy")
        # We should now have it distributed but not local
        with self.assertRaises(ImportError):
            import kubepy
        captain.launch_ship()
        sc = pyspark.context.SparkContext(master="spark://localhost:7077")
        try:
            rdd = sc.parallelize(range(2))
            def find_info(x):
                import os
                import kubepy
                import sys
                return (x, sys.executable, os.environ['PYTHONPATH'])
            result = rdd.map(find_info).collect()
        finally:
            sc.stop()
        self.assertTrue("coffee_boat/bin/python" in result[0][1])
