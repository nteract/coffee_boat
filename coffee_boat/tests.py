import pyspark
import unittest2

class TestBasicDep(unittest2.TestCase):
  def test_simple_env(self):
    from coffee_boat import Captain
    # Creat a captain
    captain = Captain()
    # Validate we don't have nbconvert installed in local context
    def fail_at_import():
      import nbconvert
    assertRaises("ImportError", fail_at_import)
    captain.add_pip_packages("pandas==0.22.0","nbconvert")
    # We should now have it....
    import nbconvert
    import pandas
    captain.launch_ship()
    sc = pyspark.context.getOrCreate()
    rdd = sc.parallelize(range(100))
    def find_info(x):
      import os
      import nbconvert
      return (x, os.environ['PYTHONPATH'])
    result = rdd.map(lambda x: find_info)
    print(result)
    assert(result == [])
