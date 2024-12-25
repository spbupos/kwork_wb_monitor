import db_conn
import json
from concurrent.futures import ThreadPoolExecutor

def main():
    config_filename = 'config.json'
    with open(config_filename) as config_file:
        config = json.load(config_file)

    # Initialize the MySQL database connection
    db_conn.mysql_db.init(
        config['database'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

    # Get APIKeys from the database
    api_keys = list(db_conn.APIKeys.select())
    instances = []
    for obj in api_keys:
        instances.append(db_conn.Writer(obj.api_key, obj.user_id, obj.runs))

    # Start the threads
    with ThreadPoolExecutor() as executor:
        for instance in instances:
            executor.submit(instance.run)

    # Wait for the threads to finish
    executor.shutdown()

    # Update in APIKeys 'runs' field with += 1
    db_conn.APIKeys.update(runs=db_conn.APIKeys.runs + 1).execute()


if __name__ == '__main__':
    main()
