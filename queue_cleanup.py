# Author: Selorm Kwami Dzakpasu

import pandas as pd
import numpy as np

# Load the Combined ISO Queues Excel file
file_path = 'Combined_ISO_Queues.xlsx'

# ISO Queue Cleanup Function
def queue_cleanup(df):
    df = df.copy()  # explicitly create a copy to avoid warnings
      
    # Step 1 - Developer Cleanup

    # Uses np.where to handle blank values and add separator only when both columns have values
    df['Entity'] = np.where(df['Interconnecting Entity'].isna(), 
                            df['Transmission Owner'], 
                            np.where(df['Transmission Owner'].isna(), 
                                    df['Interconnecting Entity'], 
                                    df['Interconnecting Entity'].astype(str) + "/" + df['Transmission Owner'].astype(str)))

    # Drops the original columns
    df.drop(['Interconnecting Entity', 'Transmission Owner'], axis=1, inplace=True)


    # Step 2 - Capacity Cleanup

    # List of the columns you want to merge into "Capacity (MW)"
    columns_to_merge = ['dp1ErisMw', 'dp1NrisMw', 'dp2ErisMw', 'dp2NrisMw', 'MW In Service']

    # Use np.where to prioritize "Capacity (MW)" if it has a value, otherwise merge the other columns
    df['Capacity (MW)'] = np.where(df['Capacity (MW)'].isna(), 
                                df[columns_to_merge].apply(lambda row: row.dropna().astype(str).iloc[0] if len(row.dropna()) > 0 else '', axis=1),
                                df['Capacity (MW)'])

    # Drops the other columns
    df.drop(columns=columns_to_merge, axis=1, inplace=True)

    # Convert "Capacity (MW)" "Winter Capacity (MW)" and "Summer Capacity (MW)" columns to integers (handling NaNs gracefully)
    for col in ['Capacity (MW)', 'Summer Capacity (MW)', 'Winter Capacity (MW)']:
        df[col] = (
            pd.to_numeric(df[col], errors='coerce')
            .fillna(0)
            .astype(int)
        )

    # Max of "Summer Capacity (MW)" and "Winter Capacity (MW)" into "Capacity (MW)"
    df['Capacity (MW)'] = (
        df[['Summer Capacity (MW)', 'Winter Capacity (MW)']]
        .max(axis=1)
        .astype(int)
    )


    # Step 3 - Unit Type Cleanup

    # List of columns to merge
    columns_to_merge = ['Generation Type', 'facilityType', 'Unit', 'Technology', 'Fuel', 'Fuel-1', 'Fuel-2', 'Fuel-3']

    # Replace "Hybrid" in "Generation Type" with "facilityType"
    df['Generation Type'] = np.where(df['Generation Type'].str.contains('Hybrid', na=False),
                                    df['facilityType'],
                                    df['Generation Type'])

    # Merge the columns, with "Generation Type" as priority, and fill missing values with others
    df['Technology'] = df[columns_to_merge].apply(lambda row: row['Generation Type'] if pd.notna(row['Generation Type']) else ' '.join(row.dropna().astype(str)), axis=1)

    # Drop the original columns used for merging
    df.drop(['Generation Type', 'facilityType', 'Unit', 'Fuel', 'Fuel-1', 'Fuel-2', 'Fuel-3'], axis=1, inplace=True)


    # Step 4 - Project Name Cleanup

    # Merge "Project Name" and "Interconnection Location" with "Interconnection Location" as priority
    df['Interconnection Location'] = np.where(df['Interconnection Location'].isna(), 
                                            df['Project Name'], 
                                            df['Interconnection Location'])

    # Merge "Project Name" and "Commercial Name" with "Commercial Name" as priority
    df['Commercial Name'] = np.where(df['Commercial Name'].isna(), 
                                df['Project Name'], 
                                df['Commercial Name'])

    # Drop the original "Project Name" column
    df.drop(columns=['Project Name'], axis=1, inplace=True)

    # Rename "Commercial Name" to "Project Name"
    df.rename(columns={'Commercial Name': 'Project Name'}, inplace=True)


    # Step 5 - Status Cleanup

    # Remove row that contains "jellyfish"
    df = df[~df.apply(lambda row: row.astype(str).str.contains('jellyfish', case=False).any(), axis=1)]

    # Rename contents of column "S" using the mapping
    s_mapping_key = { 
        "0": "Withdrawn",
        "1": "Scoping Meeting Pending",
        "2": "FES Pending",
        "3": "FES in Progress",
        "4": "SRIS/SIS Pending",
        "5": "SRIS/SIS in Progress",
        "6": "SRIS/SIS Approved",
        "7": "FS Pending",
        "8": "Rejected Cost Allocation/Next FS Pending",
        "9": "FS in Progress",
        "10": "Accepted Cost Allocation/IA in Progress",
        "11": "IA Completed",
        "12": "Under Construction",
        "13": "In Service for Test",
        "14": "In Service Commercial",
        "15": "Partial In-Service"
    }

    # Convert "S" column to integers
    df['S'] = pd.to_numeric(df['S'], errors='coerce')  # Converts to numeric (int or float), sets errors to NaN

    # Replace using s_mapping_key, where it assumes that 'S' values are now integers
    df['S'] = df['S'].apply(lambda x: s_mapping_key.get(str(int(x)), x) if pd.notna(x) else x)

    # Modify "Interconnection Agreement Status" by adding "IA " to non-empty cells
    df['Interconnection Agreement Status'] = df['Interconnection Agreement Status'].apply(
        lambda x: f"IA {x}" if pd.notna(x) and x != "" else x
    )
    
    # Remove "completed" and "Done" values from "Status" column. These do not represent the status of the project but rather the interconnection agreement.
    df.loc[df['Status'].str.contains(r'completed|done', case=False, na=False), 'Status'] = np.nan

    # Merge columns with a separator (e.g., a comma)
    columns_to_merge = ['Status', 'S', 'Status (Original)', 'Project Status', 
                        'Post Generator Interconnection Agreement Status', 'Interconnection Agreement Status']

    df['Merged Status'] = df[columns_to_merge].apply(lambda row: ' , '.join(row.dropna().astype(str)), axis=1)

    # Drop the original columns used for merging
    df.drop(['Status', 'S', 'Status (Original)', 'Project Status', 'Post Generator Interconnection Agreement Status', 'Interconnection Agreement Status'], axis=1, inplace=True)

    # Rename "Merged Status" to "Status"
    df.rename(columns={'Merged Status': 'Status'}, inplace=True)


    # Step 6 - IA Date Cleanup

    # Drop the unused date columns
    df.drop(['giaToExec', 'SGIA Tender Date', 'Interconnection Approval Date', 'Interconnection Request Receive Date', 
            'IA Signed', 'Last Updated Date', 'Updated'], axis=1, inplace=True)

    # Function to strip everything after "T" if it exists in dates
    def correct_date_format(date):
        if isinstance(date, str) and 'T' in date:
            return date.split('T')[0]
        return date

    # Apply the function to the 'Queue Date' column
    df['Queue Date'] = df['Queue Date'].apply(correct_date_format)

    # Convert the column to datetime format, then apply the Excel short date format (MM/DD/YYYY)
    df['Queue Date'] = pd.to_datetime(df['Queue Date'], errors='coerce').dt.strftime('%m/%d/%Y')


    # Step 7 - Completion/In-service Date Cleanup

    # Drop the unused columns
    df.drop(['Long Term Firm Service Start Date', 'Long Term Firm Service End Date'], axis=1, inplace=True)

    # List of planned completion related date columns
    date_columns = [
        'Actual Completion Date', 'Proposed Completion Date', 'inService',
        'Backfeed Date', 'Op Date', 'Sync Date', 'Test Energy Date', 'In-Service Date',
        'Proposed In-Service Date', 'Commercial Operation Date', 'Proposed Initial-Sync Date', 'Proposed On-line Date (as filed with IR)',
        'Approved for Energization', 'Approved for Synchronization', 'Original Generator Commercial Op Date'
    ]
        
    # Create the "Planned Operation Date" column with priority using `apply` and `lambda`
    def get_planned_operation_date(row):
        # Replace any cell with length 5 with ''
        for col in ['Actual Completion Date', 'Proposed Completion Date', 'inService'] + date_columns:
            if pd.notna(row[col]) and len(str(row[col])) == 5:
                row[col] = ''  # Replace the value with an empty string

        # Now apply the priority logic, looking for the first non-null value
        for col in ['Actual Completion Date', 'Proposed Completion Date', 'inService'] + date_columns:
            if pd.notna(row[col]) and row[col] != '':
                return row[col]
        
        return None  # In case all values are missing or replaced with ''

    df['Planned Operation Date'] = df.apply(get_planned_operation_date, axis=1)

    # Drop the original columns
    df.drop(date_columns, axis=1, inplace=True)

    # Apply the function to the 'Planned Operation Date' column
    df['Planned Operation Date'] = df['Planned Operation Date'].apply(correct_date_format)

    # Convert the column to datetime format
    df['Planned Operation Date'] = pd.to_datetime(df['Planned Operation Date'], errors='coerce')

    # Create new columns for month and year
    df['Planned Operation Month'] = df['Planned Operation Date'].dt.month
    df['Planned Operation Year'] = df['Planned Operation Date'].dt.year

    # Convert the column to datetime format, then apply the Excel short date format (MM/DD/YYYY)
    df['Planned Operation Date'] = pd.to_datetime(df['Planned Operation Date'], errors='coerce').dt.strftime('%m/%d/%Y')


    # Step 8 - Availability of Studies Cleanup (FS, SIS, etc.)

    # Drop the unused columns
    df.drop(['Feasibility Study', 'sisPhase1', 'Facilities Study', 'System Impact Study', 'Initial Study', 'Screening Study Started', 
            'Screening Study Complete', 'FIS Requested', 'FIS Approved'], axis=1, inplace=True)

    # Function to prepend "FS " if the cell is non-blank for Feasibility Study Columns
    def prepend_fs(value):
        if pd.notna(value) and value != '':
            return f"FS {value}"
        return value

    # Apply the function to the relevant columns
    columns_to_update = ['Feasibility Study Status', 'Feasiblity Study Status', 'Feasibility Study or Supplemental Review']
    for col in columns_to_update:
        df[col] = df[col].apply(prepend_fs)
        
    # Function to transform the values in the "System Impact Study Completed" column
    def transform_sis_status(value):
        if value == 'N':
            return None
        elif value == 'Y':
            return 'SIS Completed'
        return value

    # Apply the function to the "System Impact Study Completed" column
    df['System Impact Study Completed'] = df['System Impact Study Completed'].apply(transform_sis_status)

    # Function to prepend "SIS " for other System Impact Study columns if the cell is non-blank and does not contain "study"
    def prepend_sis(value):
        if pd.notna(value) and value != '' and 'study' not in value.lower():
            return f"SIS {value}"
        return value

    # Apply the function to the "System Impact Study or Phase I Cluster Study" column
    df['System Impact Study or Phase I Cluster Study'] = df['System Impact Study or Phase I Cluster Study'].apply(prepend_sis)

    # Removing false blanks in some cells
    def remove_false_blanks(row):
        # Delete contents of any cell with length 5
        for col in ['System Impact Study Status', 'Facilities Study Status']:
            if pd.notna(row[col]) and len(str(row[col])) == 5:
                row[col] = None  # Set the value to None (delete the content)
        
        return row  # Return the row after modifying it

    df = df.apply(remove_false_blanks, axis=1)

    # Apply the function to the "System Impact Study Status" column
    df['System Impact Study Status'] = df['System Impact Study Status'].apply(prepend_sis)

    # Function to prepend "FAS " Facilities Study Columns if the cell is non-blank and does not contain "SGIA"
    def prepend_fas(value):
        if pd.notna(value) and value != '':
            # If the value contains "SGIA", delete contents
            if 'SGIA' in value.upper():
                return None
            # Otherwise, prepend "FAS "
            return f"FAS {value}"
        return value

    # Apply the function to the relevant columns
    columns_to_update = ['Facilities Study Status', 'Facilities Study (FAS) or Phase II Cluster Study']
    for col in columns_to_update:
        df[col] = df[col].apply(prepend_fas)
        
    # Function to prepend "OS " if the cell is non-blank for Optional Interconnection Study columns
    def prepend_os(value):
        if pd.notna(value) and value != '':
            return f"OS {value}"
        return value

    # Apply the function to the relevant columns
    columns_to_update = ['Optional Interconnection Study Status', 'Optional Study (OS)']
    for col in columns_to_update:
        df[col] = df[col].apply(prepend_os)
        
    # Function to transform the values in the "Economic Study Required" column
    def transform_es(value):
        if value == 'No':
            return 'Economic Study Waived'
        elif value == 'Yes':
            return 'Economic Study Required'
        return value

    # Apply the function to the "Economic Study Required" column
    df['Economic Study Required'] = df['Economic Study Required'].apply(transform_es)

    # Function to make the cell blank if it contains "GIA" for "studyPhase" column
    def replace_gia(value):
        if pd.notna(value) and 'GIA' in value.upper():
            return None
        return value

    # Apply the function to the "studyPhase" column
    df['studyPhase'] = df['studyPhase'].apply(replace_gia)

    # List of columns to concatenate
    columns_to_concatenate = [
        'Availability of Studies', 'GIM Study Phase', 'Feasibility Study Status', 'Feasiblity Study Status',
        'Feasibility Study or Supplemental Review', 'System Impact Study Completed',
        'System Impact Study or Phase I Cluster Study', 'System Impact Study Status',
        'Facilities Study Status', 'Facilities Study (FAS) or Phase II Cluster Study',
        'Optional Interconnection Study Status', 'Optional Study (OS)', 'Economic Study Required', 'studyPhase', 'studyCycle'
    ]

    # Concatenate the columns
    df['Concatenated Studies'] = df[columns_to_concatenate].apply(lambda row: ' , '.join(row.dropna().astype(str)), axis=1)

    # Drop the original columns
    df.drop(columns_to_concatenate, axis=1, inplace=True)

    # Rename "Concatenated Studies" to "Availability of Studies"
    df.rename(columns={'Concatenated Studies': 'Availability of Studies'}, inplace=True)


    # Step 9 - Capacity Related Statuses Cleanup

    # Concatenate 'Full Capacity, Partial or Energy Only (FC/P/EO)' and 'Off-Peak Deliverability and Economic Only' columns
    df['Capacity Status'] = df[['Full Capacity, Partial or Energy Only (FC/P/EO)', 'Off-Peak Deliverability and Economic Only']
                            ].apply(lambda row: ' , '.join(row.dropna().astype(str)), axis=1)

    # Drop the original columns
    df.drop(['Full Capacity, Partial or Energy Only (FC/P/EO)', 'Off-Peak Deliverability and Economic Only'], axis=1, inplace=True)


    # Step 10 - Cluster Group Cleanup

    # Concatenate 'Cluster Group', 'CDR Reporting Zone' and 'studyGroup' columns
    df['Group'] = df[['Cluster Group', 'CDR Reporting Zone', 'studyGroup']
                            ].apply(lambda row: ' , '.join(row.dropna().astype(str)), axis=1)

    # Drop the original columns
    df.drop(['Cluster Group', 'CDR Reporting Zone', 'studyGroup'], axis=1, inplace=True)


    # Step 11 - Service Type Cleanup

    # Use np.where to prioritize "Service Type" if it has a value, otherwise merge the other columns ('Serv' and 'svcType')
    df['Service Type'] = np.where(df['Service Type'].isna(), 
                                df[['svcType', 'Serv']].apply(lambda row: row.dropna().astype(str).iloc[0] if len(row.dropna()) > 0 else '', axis=1),
                                df['Service Type'])

    # Drops the other columns
    df.drop(['svcType', 'Serv'], axis=1, inplace=True)


    # Step 12 - Cleanup of Currently Irrelevant Data

    # Drop irrelevant columns based on current research needs and goals
    df.drop(['Air Permit', 'GHG Permit', 'Water Availability', 'I39', 'Meets Planning', 'Meets All Planning', 'Interim-Interconnection Service-Generation Interconnection Agreement',
            'Interim-Interconnection Service-Generation Interconnection Agreement-Status', 'Wholesale Market Participation Agreement', 'Construction Service Agreement',
            'Construction Service Agreement Status', 'Upgrade Construction Service Agreement', 'Upgrade Construction Service Agreement Status', 'Withdrawal Comment',
            'Study Process', 'Z', 'Dev', 'Zone'], axis=1, inplace=True)


    # Step 13 - Reorder columns in dataframe

    # Rename "Withdrawn Date" to "Withdrawal Date"
    df.rename(columns={'Withdrawn Date': 'Withdrawal Date'}, inplace=True)

    new_column_order = [
        'Queue ID', 'Project Name', 'Entity', 'Queue Date', 'State', 'County', 'Latitude', 'Longitude',
        'Service Type', 'Status', 'Interconnection Location', 'Planned Operation Date', 
        'Planned Operation Month', 'Planned Operation Year', 'Technology', 
        'Type-1', 'Type-2', 'Type-3', 'Capacity (MW)', 'MW-1', 'MW-2', 'MW-3', 
        'Summer Capacity (MW)', 'Winter Capacity (MW)', 'Capacity Status', 
        'Availability of Studies', 'Balancing Authority Code', 'Balancing Authority Name',
        'Current Cluster', 'Group', 'Cessation Date', 'Withdrawal Date', 'Comment'
    ]

    # Reorder the DataFrame columns
    df = df[new_column_order]
    
    return df


# Using the cleanup function to clean active, withdrawn and completed entries

# Load the first sheet into a pandas DataFrame (this contains active entries)
df = pd.read_excel(file_path, sheet_name="Active")
# run queue cleanup function and move dataframe to new variable
df_active = queue_cleanup(df)


# Load the second sheet into a pandas DataFrame (this contains withdrawn entries)
df = pd.read_excel(file_path, sheet_name="Withdrawn")
# run queue cleanup function and move dataframe to new variable
df_withdrawn = queue_cleanup(df)


# Load the third sheet into a pandas DataFrame (this contains completed entries)
df = pd.read_excel(file_path, sheet_name="Completed")
# run queue cleanup function and move dataframe to new variable
df_completed = queue_cleanup(df)


# Save the updated DataFrames back to an Excel file
with pd.ExcelWriter('Cleaned_ISO_Queues.xlsx') as writer:
    df_active.to_excel(writer, sheet_name='Active', index=False)
    df_withdrawn.to_excel(writer, sheet_name='Withdrawn', index=False)
    df_completed.to_excel(writer, sheet_name='Completed', index=False)    

