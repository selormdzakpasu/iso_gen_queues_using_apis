Author: Selorm Kwami Dzakpasu

Date: 1/23/2025

Topic: Compilation of Generator Interconnection Queues from the Seven ISOs (CAISO, ERCOT, MISO, PJM, SPP, NYISO & NEISO) using APIs

Things to note:

* The script uses a python library called "gridstatus" to fetch the interconnection queues from all the ISOs with the exception of PJM. Tested with gridstatus version 0.28.0!

* Gridstatus version 0.28.0 currently (1/23/2025) only works with Python 3.11 so you'll need to have a parallel installation of this version if you have a higher version installed. If you have a lower version you'll need to update to Python 3.11.

* PJM's website works funny sometimes so if the script fails just run it again. It should work on the next attempt.

* To use this resource:
1. Install the following python libraries: gridstatus and pandas. These call be installed using pip.
2. You can change the final exported excel file name and path in "main.py" if required. It defaults to the script folder.
3. Run "main.py".

Enjoy!