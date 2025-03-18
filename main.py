# Author: Selorm Kwami Dzakpasu

import gridstatus # Only compatible with Python 3.11
import requests
import pandas as pd
from gridstatus import utils
from gridstatus.base import InterconnectionQueueStatus
import re

# PJM Get Queue Function
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

# SPP Get Queue Function    
def get_spp_interconnection_queue():
    """Get interconnection queue

    Returns:
        pandas.DataFrame: Interconnection queue
    """
    url = "https://opsportal.spp.org/Studies/GenerateSummaryCSV"
    response = requests.get(url)
    raw_data = utils.get_response_blob(response)
    
    queue = pd.read_csv(raw_data, skiprows=1)

    queue["Status (Original)"] = queue["Status"]
    completed_val = InterconnectionQueueStatus.COMPLETED.value
    active_val = InterconnectionQueueStatus.ACTIVE.value
    withdrawn_val = InterconnectionQueueStatus.WITHDRAWN.value
    queue["Status"] = queue["Status"].map(
        {
            "IA FULLY EXECUTED/COMMERCIAL OPERATION": completed_val,
            "IA FULLY EXECUTED/ON SCHEDULE": completed_val,
            "IA FULLY EXECUTED/ON SUSPENSION": completed_val,
            "IA PENDING": active_val,
            "DISIS STAGE": active_val,
            "None": active_val,
            "WITHDRAWN": withdrawn_val,
        },
    )

    queue["Generation Type"] = queue[["Generation Type", "Fuel Type"]].apply(
        lambda x: " - ".join(x.dropna()),
        axis=1,
    )

    queue["Proposed Completion Date"] = queue["Commercial Operation Date"]

    rename = {
        "Generation Interconnection Number": "Queue ID",
        " Nearest Town or County": "County",
        "State": "State",
        "TO at POI": "Transmission Owner",
        "Capacity": "Capacity (MW)",
        "MAX Summer MW": "Summer Capacity (MW)",
        "MAX Winter MW": "Winter Capacity (MW)",
        "Generation Type": "Generation Type",
        "Request Received": "Queue Date",
        "Substation or Line": "Interconnection Location",
        "Date Withdrawn": "Withdrawn Date",
    }

    # todo: there are a few columns being parsed
    # as "unamed" that aren't being included but should
    extra_columns = [
        "In-Service Date",
        "Commercial Operation Date",
        "Cessation Date",
        "Current Cluster",
        "Cluster Group",
        "Original Generator Commercial Op Date",
        "Service Type",
        "Status (Original)",
    ]

    missing = [
        "Project Name",
        "Interconnecting Entity",
        "Withdrawal Comment",
        "Actual Completion Date",
    ]

    queue = utils.format_interconnection_df(
        queue=queue,
        rename=rename,
        extra=extra_columns,
        missing=missing,
    )

    return queue

# NYISO
nyiso = gridstatus.NYISO()
nyiso_queue = nyiso.get_interconnection_queue()
nyiso_queue['Balancing Authority Code'] = 'NYISO' # Adding Balancing Authority Code column
nyiso_queue['Balancing Authority Name'] = 'New York Independent System Operator' # Adding Balancing Authority Name column
nyiso_queue['Latitude'] = None # Adding Latitude column
nyiso_queue['Longitude'] = None # Adding Longitude column
# nyiso_queue.to_excel("test.xlsx", index=False, engine='openpyxl')


# CAISO
caiso = gridstatus.CAISO()
caiso_queue = caiso.get_interconnection_queue()
caiso_queue['Balancing Authority Code'] = 'CAISO'
caiso_queue['Balancing Authority Name'] = 'California Independent System Operator'
caiso_queue['Latitude'] = None
caiso_queue['Longitude'] = None
# caiso_queue.to_excel("test1.xlsx", index=False, engine='openpyxl')


# SPP
spp_queue = get_spp_interconnection_queue()
spp_queue['Balancing Authority Code'] = 'SPP'
spp_queue['Balancing Authority Name'] = 'Southwest Power Pool'
spp_queue['Latitude'] = None 
spp_queue['Longitude'] = None 
# spp_queue.to_excel("test2.xlsx", index=False, engine='openpyxl')


# ERCOT
ercot = gridstatus.Ercot()
ercot_queue = ercot.get_interconnection_queue()
ercot_queue['Balancing Authority Code'] = 'ERCOT'
ercot_queue['Balancing Authority Name'] = 'Electric Reliability Council of Texas'
ercot_queue['Latitude'] = None
ercot_queue['Longitude'] = None 
# ercot_queue.to_excel("test3.xlsx", index=False, engine='openpyxl')


# MISO
miso = gridstatus.MISO()
miso_queue = miso.get_interconnection_queue()
miso_queue['Balancing Authority Code'] = 'MISO'
miso_queue['Balancing Authority Name'] = 'Midcontinent Independent Transmission System Operator'
miso_queue['Latitude'] = None
miso_queue['Longitude'] = None 
# miso_queue.to_excel("test4.xlsx", index=False, engine='openpyxl')


# NEISO
isone = gridstatus.ISONE()
neiso_queue = isone.get_interconnection_queue()
neiso_queue['Balancing Authority Code'] = 'NEISO'
neiso_queue['Balancing Authority Name'] = 'New England Independent System Operator'
neiso_queue['Latitude'] = None
neiso_queue['Longitude'] = None 
# neiso_queue.to_excel("test5.xlsx", index=False, engine='openpyxl')


# PJM
# pjm_queue = requests.get("https://www.pjm.com/pjmfiles/media/planning/queues-data/PlanningQueues.xml")
# with open("pjm_queue.xml", "wb") as f:
#     f.write(pjm_queue.content) # Saves fetched file

pjm_queue = get_pjm_interconnection_queue()
pjm_queue['Balancing Authority Code'] = 'PJM'
pjm_queue['Balancing Authority Name'] = 'PJM Interconnection, LLC'
pjm_queue['Latitude'] = None
pjm_queue['Longitude'] = None 
# pjm_queue.to_excel("test6.xlsx", index=False, engine='openpyxl')


# Combine all queues
queues = [nyiso_queue, caiso_queue, spp_queue, ercot_queue, miso_queue, neiso_queue, pjm_queue]
combined_df = pd.concat(queues, ignore_index=True)

# List of statuses to remove (Withdrawn/Deactivated)
statuses_withdrawn = [
    "Annulled", "Canceled", "Deactivated", "Retracted", "Suspended", "WITHDRAWN", "Withdrawn"
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

# Create a DataFrame for entries with in-service status
completed_df_1 = combined_df[combined_df["Status"] == "In Service"]
active_df_1 = combined_df[combined_df["Status"] != "In Service"]

completed_df_2 = active_df_1[active_df_1["Status (Original)"] == "IA FULLY EXECUTED/COMMERCIAL OPERATION"]
active_df_2 = active_df_1[active_df_1["Status (Original)"] != "IA FULLY EXECUTED/COMMERCIAL OPERATION"]

completed_df_3 = active_df_2[active_df_2["Project Status"] == "In Service"]
active_df_3 = active_df_2[active_df_2["Project Status"] != "In Service"]

# Ensure the "S" column is converted to integers
completed_df_4 = active_df_3[active_df_3["S"] == 14] # "14" represents "In Service Commercial"
active_df_4 = active_df_3[active_df_3["S"] != 14]

completed_df_5 = active_df_4[active_df_4["Post Generator Interconnection Agreement Status"] == "In Service"]
active_df_5 = active_df_4[active_df_4["Post Generator Interconnection Agreement Status"] != "In Service"]

completed = [completed_df_1, completed_df_2, completed_df_3, completed_df_4, completed_df_5]
completed_df = pd.concat(completed, ignore_index=True)

# Active entries remain after removing rows with in service status from the main dataframe
active_df = active_df_5

# Export DataFrames to an Excel file
with pd.ExcelWriter("Combined_ISO_Queues.xlsx", engine='openpyxl') as writer: # "Combined_ISO_Queues.xlsx" can be replaced with the desired file path & name
    active_df.to_excel(writer, index=False, sheet_name="Active") # Active entries
    withdrawn_df.to_excel(writer, index=False, sheet_name="Withdrawn") # Withdrawn/deactivated entries
    completed_df.to_excel(writer, index=False, sheet_name="Completed") # Entries with completed/in-service status

# Run Queue Cleanup
exec(open("queue_cleanup.py").read())