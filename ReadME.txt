Author: Selorm Kwami Dzakpasu

Date: 2/11/2025

Topic: Compilation of Generator Interconnection Queues from the Seven ISOs (CAISO, ERCOT, MISO, PJM, SPP, NYISO & NEISO) using APIs

Things to note:

* The script uses a Python library called "gridstatus" to fetch the interconnection queues from all the ISOs with the exception of PJM and SPP. 
  However, gridstatus is used for the initial formatting of all the ISO queues.
  Tested with gridstatus version 0.28.0!

* Gridstatus version 0.28.0 currently (2/11/2025) only works with Python 3.11 so you'll need to have a parallel installation of this version if you have a higher version installed. 
  If you have a lower version, you'll need to update to Python 3.11.

* PJM's website works funny sometimes, so if the script fails just run it again. It should work on the next attempt.

* Gridstatus has also proven to be a little unreliable occasionally. 
Example: Following my initial release of this script (1/23/2025), there was a change to the SPP Interconnection Queue which gridstatus was not robust enough to handle.
As such, I had to import the function handling this query from the gridstatus library and make modifications in order to obtain and process the SPP Queue.
Without this, gridstatus would have had to fix the issue before this script would be able to run again without errors. 
If the script fails on three consecutive attempts, gridstatus may be the problem. Log the errors and address it following how the PJM and SPP queues have been dealt with in main.py.
Note that it will not follow the exact same steps so do well to use the respective files within the gridstatus library (https://github.com/gridstatus/gridstatus/tree/main/gridstatus) to find the relevant functions.

* To use this resource:
1. Install the following python libraries: gridstatus and pandas. These can be installed using pip.
2. You can change the final exported excel file name and path in "main.py" for the combined queues and "queue_cleanup.py" for the cleaned queues if required. It defaults to the script folder.
3. Run "main.py".

Enjoy!