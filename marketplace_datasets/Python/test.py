import configparser

# Read the SnowSQL config file
config = configparser.ConfigParser()
config.read('/Users/sebastianwiesner/.snowsql/config')

# Retrieve the connection details
account = config.get('connections.NIMBUS', 'accountname')
username = config.get('connections.NIMBUS', 'username')
password = config.get('connections.NIMBUS', 'password')
database = config.get('connections.NIMBUS', 'dbname')
schema = config.get('connections.NIMBUS', 'schemaname')

# Print the connection details
print(f"Account: {account}")
print(f"Username: {username}")
print(f"Password: {password}")
print(f"Database: {database}")
print(f"Schema: {schema}")