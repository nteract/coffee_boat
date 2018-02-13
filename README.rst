|coffee_boat_logo| |buildstatus|

pyspark-deps
============

WIP PySpark dependency management.

Tests
-----

Running tests will modify your current virtualenv’s packages. This
software is WIP.

Using
-----

Not in production, please! If you’re curious to see how it can be used,
check out
https://github.com/nteract/coffee_boat/blob/master/Coffee%2BBoat%2BSample.ipynb

This has been tested (although not thoroughly) with standalone mode and
YARN. It explicitly does not currently support local mode, although we’d
love your contribs.

For now, stay tuned or join us in hacking!

Alternatives
------------

Build you own conda env, or use one of the commercial integrated
solutions which has a nice UI for this.

.. |coffee_boat_logo| image:: https://raw.githubusercontent.com/nteract/coffee_boat/master/imgs/coffee-boat.png
.. |buildstatus| image:: https://travis-ci.org/nteract/coffee_boat.svg?branch=master
   :target: https://travis-ci.org/nteract/coffee_boat
