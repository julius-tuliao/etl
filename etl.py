import time
import configparser
import psycopg2

class Replicator:
    def __init__(self, config_file):
        # Parse the configuration file
        config = configparser.ConfigParser()
        config.read(config_file)

        # Read the connection details and other configuration parameters from the configuration file
        self.source_host = config['source']['host']
        self.source_db = config['source']['database']
        self.source_user = config['source']['user']
        self.source_password = config['source']['password']
        self.destination_host = config['destination']['host']
        self.destination_db = config['destination']['database']
        self.destination_user = config['destination']['user']
        self.destination_password = config['destination']['password']

    def replicate(self, table_name):
        # Connect to the source database
        conn_source = psycopg2.connect(host=self.source_host, database=self.source_db, user=self.source_user, password=self.source_password)
        cursor_source = conn_source.cursor()

        # Connect to the destination database
        conn_destination = psycopg2.connect(host=self.destination_host, database=self.destination_db, user=self.destination_user, password=self.destination_password)
        cursor_destination = conn_destination.cursor()

        # Retrieve the last replication timestamp from the destination table
        cursor_destination.execute(f'SELECT MAX(last_updated) FROM {table_name}')
        last_replication_timestamp = cursor_destination.fetchone()[0]

        # Execute a SELECT query to retrieve the modified rows from the source table
        cursor_source.execute(f'SELECT * FROM {table_name} WHERE last_updated > %s', (last_replication_timestamp,))

        # Iterate through the modified rows and insert or update them in the destination table
        for row in cursor_source:
            cursor_destination.execute(f'SELECT * FROM {table_name} WHERE id = %s', (row[0],))
            if cursor_destination.fetchone() is None:
                cursor_destination.execute(f'INSERT INTO {table_name} VALUES (%s, %s, %s, %s)', row)
            else:
                cursor_destination.execute(f'UPDATE {table_name} SET column1 = %s, column2 = %s, last_updated = %s WHERE id = %s', (row[1], row[2], row[3], row[0]))

        # Commit the changes to the destination database
        conn_destination.commit()

        # Close the cursors and connections
        cursor_source.close()
        cursor_destination.close()
        conn_source.close()
        conn_destination.close()


# Create a Replicator object with the desired connection details
replicator = Replicator('replication.ini')
# Run the replication process in a loop, with a 10-minute delay between each iteration
while True:
    replicator.replicate('table')
    time.sleep(600)