import pandas as pd
import pyodbc

# This first connection to the SQL Server is used to query for the
# Job run times (Query STring at bttom).
def send_to_sql_server(query):
    """ Connect to server, run stored procedure """
    # TODO this should utilize a context-manager
    connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                        "Server=BISQLDEV;"
                        "Database=NSightDW;"
                        "Trusted_Connection=yes;") # Trusted_Connection uses your windows creds

    # This uses the above connection to run the query passed in and return the data
    # as a Pandas Dataframe
    data = pd.read_sql(query, connection)
    
    # Close connection and return data to what call this function
    connection.close()
    return data

# This connection is used to Truncate/Write the SQL Server, the queries are within this function
def write_to_sql_server(dataframe):
    """ Connect to server, load dataframe into table """
    # TODO this should utilize a context-manager
    connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                        "Server=BISQLDEV;"
                        "Database=NSightDW;"
                        "Trusted_Connection=yes;")

    # Creates a cursor to perform database actions
    cursor = connection.cursor()

    # Use cursor to trucate Table before filling it with the modified data got from the first connection
    cursor.execute("TRUNCATE TABLE [Baseline].[Job_Median_Duration]")

    # This moves through the dataframe that is passed to this function row by row and Inserts them into the SQL table
    for row in dataframe.iterrows():
        cursor.execute("INSERT INTO [Baseline].[Job_Median_Duration]([Name], [MedianDuration]) \
                         values (?, ?)",row[1][0], 
                                        row[1][1])

        # This is necessary to save the changes that have been  made in SQL Server
        connection.commit()

    # Disconnecting the cursor and the connection
    cursor.close()
    connection.close()

# Query used to get the initial data aout the jobs
query_String = r"""WITH DurationData AS (
	SELECT
		sj.name,
		run_duration,
		rn = row_number() over (partition by sj.name order by sh.run_date desc)
	FROM msdb.dbo.sysjobs sj
    JOIN msdb.dbo.sysjobhistory sh
    ON sj.job_id = sh.job_id
    WHERE 
        sj.name LIKE 'NSight Staging SAP%'
        AND
        step_name LIKE '%(job outcome)%'
        AND
        sj.name NOT LIKE '%Monthly%'
)
SELECT
	name,
	run_duration
FROM
	DurationData
WHERE
	rn <=21"""

# Send above query the the first server connection and save the returned dataframe (A dataframe is a table) as df
df = send_to_sql_server(query_String)

# perform a group by on the job name column (without making it the index) and aggregate the
# run durations as a median and store the new dataframe as median_df
median_df = df.groupby(by='name', as_index=False)[['run_duration']].median()

# Send the new dataframe to the second connection to be written to the table in the SQL Server
write_to_sql_server(median_df)