import pyspark
import unittest2

class TestBasicDep(unittest2.TestCase):
  """Test we package dependencies. Note: this test case is EVIL!"""

  def test_simple_env(self):
    from coffee_boat import Captain
    # Creat a captain
    captain = Captain(accept_conda_license=True)
    # Validate we don't have nbconvert installed in local context
    import subprocess
    subprocess.call(["pip", "uninstall", "-y", "nbconvert"])
    with self.assertRaises(ImportError):
      import nbconvert
    captain.add_pip_packages("pandas==0.22.0","nbconvert")
    # We should now have it....
    import nbconvert
    import pandas
    captain.launch_ship()
    sc = pyspark.context.SparkContext.getOrCreate()
    rdd = sc.parallelize(range(2))
    def find_info(x):
      import os
      import nbconvert
      import sys
      return (x, sys.executable, os.environ['PYTHONPATH'], os.listdir('.'))
    result = rdd.map(find_info).collect()
    print("RESULT IS {0}".format(result))
    sc.stop()
    self.assertEqual(result, [])


  def boop_non_local_env(self):
    from coffee_boat import Captain
    # Creat a captain
    captain = Captain(install_local=False, accept_conda_license=True)
    # Validate we don't have nbconvert installed in local context
    import subprocess
    subprocess.call(["pip", "uninstall", "-y", "nbconvert"])
    with self.assertRaises(ImportError):
      import nbconvert
    captain.add_pip_packages("pandas==0.22.0","nbconvert")
    # We should now have it....
    import nbconvert
    import pandas
    captain.launch_ship()
    sc = pyspark.context.SparkContext.getOrCreate()
    rdd = sc.parallelize(range(2))
    def find_info(x):
      import os
      import nbconvert
      import sys
      return (x, sys.executable, os.environ['PYTHONPATH'])
    result = rdd.map(find_info).collect()
    print("RESULT IS {0}".format(result))
    self.assertEqual(result, [])
    with self.assertRaises(ImportError):
      import nbconvert
