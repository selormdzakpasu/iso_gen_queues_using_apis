# Author: Selorm Kwami Dzakpasu

import gridstatus # Only compatible with Python 3.11
import requests
import pandas as pd
from gridstatus import utils
import re

def get_pjm_interconnection_queue():
    
    # Fetch the XML data from the URL
    url = "https://www.pjm.com/pjmfiles/media/planning/queues-data/PlanningQueues.xml"
    response = requests.get(url)

    # Use pandas to read the XML content directly into a DataFrame
    queue = pd.read_xml(response.content)
    
    # Update column names: add spaces between capital letters
    queue.columns = [re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', col) for col in queue.columns]
    
    # Add spaces after "MW"
    queue.columns = [re.sub(r'(?<=MW)(?=[A-Z])', ' ', col) for col in queue.columns]

    # Add spaces before "or"
    queue.columns = [re.sub(r'(?<=\w)(or)(?=\w)', ' or ', col) for col in queue.columns]
    
    # print(queue.columns)

    queue["Capacity (MW)"] = queue[["Maximum Facility Output", "MW In Service"]].min(axis=1)

    rename = {
        "Project Number": "Queue ID",
        "Name": "Project Name",
        "County": "County",
        "State": "State",
        "Transmission Owner": "Transmission Owner",
        "Submitted Date": "Queue Date",
        "Withdrawal Date": "Withdrawn Date",
        "Withdrawn Remarks": "Withdrawal Comment",
        "Status": "Status",
        "Revised In Service Date": "Proposed Completion Date",
        "Actual In Service Date": "Actual Completion Date",
        "Fuel": "Generation Type",
        "MW Capacity": "Summer Capacity (MW)",
        "MW Energy": "Winter Capacity (MW)",
        "Project Type": "Service Type"
    }

    extra = [
        "Service Type",
        "MW In Service",
        "Commercial Name",
        "Initial Study",
        "Feasibility Study",
        "Feasibility Study Status",
        "System Impact Study",
        "System Impact Study Status",
        "Facilities Study",
        "Facilities Study Status",
        "Interim-Interconnection Service-Generation Interconnection Agreement",
        "Interim-Interconnection Service-Generation Interconnection Agreement-Status",
        "Wholesale Market Participation Agreement",
        "Construction Service Agreement",
        "Construction Service Agreement Status",
        "Upgrade Construction Service Agreement",
        "Upgrade Construction Service Agreement Status",
        "Backfeed Date",
        "Long Term Firm Service Start Date",
        "Long Term Firm Service End Date",
        "Test Energy Date"
    ]

    missing = ["Interconnecting Entity", "Interconnection Location"]

    queue = utils.format_interconnection_df(
        queue,
        rename,
        extra=extra,
        missing=missing,
    )
    
    queue = queue[queue["Service Type"] == "Generation Interconnection"] # Returns only Generation Interconnection entries

    return queue


# NYISO
nyiso = gridstatus.NYISO()
nyiso_queue = nyiso.get_interconnection_queue()
# nyiso_queue.to_excel("test.xlsx", index=False, engine='openpyxl')


# CAISO
caiso = gridstatus.CAISO()
caiso_queue = caiso.get_interconnection_queue()
# caiso_queue.to_excel("test1.xlsx", index=False, engine='openpyxl')


# SPP
spp = gridstatus.SPP()
spp_queue = spp.get_interconnection_queue()
# spp_queue.to_excel("test2.xlsx", index=False, engine='openpyxl')


# ERCOT
ercot = gridstatus.Ercot()
ercot_queue = ercot.get_interconnection_queue()
# ercot_queue.to_excel("test3.xlsx", index=False, engine='openpyxl')


# MISO
miso = gridstatus.MISO()
miso_queue = miso.get_interconnection_queue()
# miso_queue.to_excel("test4.xlsx", index=False, engine='openpyxl')


# NEISO
isone = gridstatus.ISONE()
neiso_queue = isone.get_interconnection_queue()
# neiso_queue.to_excel("test5.xlsx", index=False, engine='openpyxl')


# PJM
# pjm_queue = requests.get("https://www.pjm.com/pjmfiles/media/planning/queues-data/PlanningQueues.xml")
# with open("pjm_queue.xml", "wb") as f:
#     f.write(pjm_queue.content) # Saves fetched file

pjm_queue = get_pjm_interconnection_queue()
# pjm_queue.to_excel("test6.xlsx", index=False, engine='openpyxl')


# Combine all queues
queues = [nyiso_queue, caiso_queue, spp_queue, ercot_queue, miso_queue, neiso_queue, pjm_queue]
combined_df = pd.concat(queues, ignore_index=True)

# List of statuses to remove (Withdrawn/Deactivated)
statuses_withdrawn = [
    "Annulled", "Canceled", "Deactivated", "Retracted", "Suspended", "WITHDRAWN", "Withdrawn"
]

# List of statuses to remove (Completed/In-Service)
statuses_completed = [
    "Completed", "Confirmed", "Done", "In Service", "COMPLETED"
]

# Create a DataFrame for entries with withdrawn status
withdrawn_df_1 = combined_df[combined_df["Status"].isin(statuses_withdrawn)] # Check "Status" for withdrawn entries
not_withdrawn_df = combined_df[~combined_df["Status"].isin(statuses_withdrawn)]

withdrawn_df_2 = not_withdrawn_df[not_withdrawn_df["Status (Original)"] == "TERMINATED"] # Check "Status (Original)" for terminated entries

withdrawn = [withdrawn_df_1, withdrawn_df_2]
withdrawn_df = pd.concat(withdrawn, ignore_index=True)

# Remove rows with withdrawn/terminated status from the main dataframe
combined_df = combined_df[~combined_df["Status"].isin(statuses_withdrawn)]
combined_df = combined_df[combined_df["Status (Original)"] != "TERMINATED"]

# Create a DataFrame for entries with in-service/completed status
completed_df = combined_df[combined_df["Status"].isin(statuses_completed)]

# Remove rows with completed status from the main dataframe
combined_df = combined_df[~combined_df["Status"].isin(statuses_completed)]

# Export DataFrames to an Excel file
with pd.ExcelWriter("Combined_ISO_Queues.xlsx", engine='openpyxl') as writer: # "Combined_ISO_Queues.xlsx" can be replaced with the desired file path & name
    combined_df.to_excel(writer, index=False, sheet_name="Active") # Active entries
    withdrawn_df.to_excel(writer, index=False, sheet_name="Withdrawn") # Withdrawn/deactivated entries
    completed_df.to_excel(writer, index=False, sheet_name="Completed") # Entries with completed/in-service status