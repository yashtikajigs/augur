import pytest
import docker
import subprocess
import json
import os
from workers.worker_git_integration import *
from workers.facade_worker.contributor_interfaceable.contributor_interface import *


# utility functions
def poll_database_connection(database_string):
    print("Attempting to create db engine")

    db = s.create_engine(database_string, poolclass=s.pool.NullPool,
                         connect_args={'options': '-csearch_path={}'.format('augur_data')})

    return db


def insert_sql_file(database_connection, fileString):
    fd = open(fileString, 'r')

    sqlFile = fd.read()
    fd.close()

    # Get list of commmands
    sqlCommands = sqlFile.split(';')

    #Iterate and execute
    for command in sqlCommands:
        toExecute = s.sql.text(command)
        try:
            pd.read_sql(
                toExecute, database_connection, params={})
        except Exception as e:
            print(f"Error when inserting data: {e}")


def insert_json_file(database_connection, fileString, table):
    jsonFile = open(fileString)

    source_data = json.load(jsonFile)

    jsonFile.close()

    # Actually insert the data to the table object passed in.
    database_connection.execute(table.insert().values(source_data))


# database connection
@pytest.fixture
def database_connection_string():
    # Create client to docker daemon
    client = docker.from_env()

    print("Building the database container...")

    cwd = os.getcwd()
    # Build the test database from the dockerfile and download
    # Postgres docker image if it doesn't exist.
    ROOT_AUGUR_DIR = os.path.dirname(
        os.path.dirname(os.path.realpath(__file__)))
    ROOT_AUGUR_DIR = str(ROOT_AUGUR_DIR).split("augur")
    ROOT_AUGUR_DIR = ROOT_AUGUR_DIR[0] + "augur"

    buildString = ROOT_AUGUR_DIR

    # change to root augur directory
    os.chdir(ROOT_AUGUR_DIR)

    print(os.getcwd())

    image = client.images.build(
        path=buildString, dockerfile="util/docker/database/Dockerfile", pull=True)

    #Handle if the port is being used first
    tries = 5
    port = 5400
    
    while tries > 0:
        
        try:
            # Start a container and detatch
            # Wait until the database is ready to accept connections
            databaseContainer = client.containers.run(image[0].id, command=None, ports={
                                                '5432/tcp': port}, detach=True)

            databaseContainer.rename("Test_database")
        except docker.errors.APIError:
            tries -= 1
            port += 1
            
            #Kill if not able to get a port
            if tries <= 0:
                print("Not able to get a port!")
                raise RuntimeError
            
            continue
        
        break

    DB_STR = 'postgresql://{}:{}@{}:{}/{}'.format(
        "augur", "augur", "172.17.0.1", port, "test"
    )

    time.sleep(10)

    #Example of how to create a new connection from the DB_STR
    #db = poll_database_connection(DB_STR)

    # Setup complete, return the database object
    yield DB_STR

    # Cleanup the docker container by killing it.
    databaseContainer.kill()
    # Remove the name
    databaseContainer.remove()

@pytest.fixture
def database_connection(database_connection_string):
    db = poll_database_connection(database_connection_string)
    
    return db  

# Define a dummy worker class that gets the methods we need without running super().__init__


class DummyPersistance(Persistant):
    def __init__(self, database_connection):
        self.db = database_connection
        self.logger = logging.getLogger()



# Dummy for the rest of the worker's methods and functionality including the facade g
class DummyFullWorker(ContributorInterfaceable):
    def __init__(self, database_connection_string, config={}):

        self.db_str = database_connection_string
        
        print(self.db_str)
        
        # Get a way to connect to the docker database.
        self.db = poll_database_connection(database_connection_string)
        self.logger = logging.getLogger()

        worker_type = "contributor_interface"

        self.data_tables = ['contributors', 'pull_requests', 'commits',
                            'pull_request_assignees', 'pull_request_events', 'pull_request_labels',
                            'pull_request_message_ref', 'pull_request_meta', 'pull_request_repo',
                            'pull_request_reviewers', 'pull_request_teams', 'message', 'pull_request_commits',
                            'pull_request_files', 'pull_request_reviews', 'pull_request_review_message_ref',
                            'contributors_aliases', 'unresolved_commit_emails']
        self.operations_tables = ['worker_history', 'worker_job']

        self.platform = "github"
        # first set up logging.
        self._root_augur_dir = Persistant.ROOT_AUGUR_DIR
        self.augur_config = AugurConfig(self._root_augur_dir)

        # Get default logging settings
        self.config = config

        self.config.update({
            'gh_api_key': self.augur_config.get_value('Database', 'key'),
            'gitlab_api_key': self.augur_config.get_value('Database', 'gitlab_api_key')
            # 'port': self.augur_config.get_value('Workers', 'contributor_interface')
        })

        # Use a special method overwrite to initialize the values for docker connection.
        self.initialize_database_connections()

        self.tool_source = '\'Dummy GithubInterfaceable Worker\''
        self.tool_version = '\'1.0.1\''
        self.data_source = '\'Worker test Data\''

    # This mirros the functionality of the definition found in worker_persistance to make
    # github related function calls much much easier to test.
    def initialize_database_connections(self):
        DB_STR = self.db_str

        self.db_schema = 'augur_data'
        self.helper_schema = 'augur_operations'

        self.helper_db = s.create_engine(DB_STR, poolclass=s.pool.NullPool,
                                         connect_args={'options': '-csearch_path={}'.format(self.helper_schema)})

        metadata = s.MetaData()
        helper_metadata = s.MetaData()

        # Reflect only the tables we will use for each schema's metadata object
        metadata.reflect(self.db, only=self.data_tables)
        helper_metadata.reflect(self.helper_db, only=self.operations_tables)

        Base = automap_base(metadata=metadata)
        HelperBase = automap_base(metadata=helper_metadata)

        Base.prepare()
        HelperBase.prepare()
        # So we can access all our tables when inserting, updating, etc
        for table in self.data_tables:
            setattr(self, '{}_table'.format(table),
                    Base.classes[table].__table__)

        try:
            self.logger.info(HelperBase.classes.keys())
        except:
            pass

        for table in self.operations_tables:
            try:
                setattr(self, '{}_table'.format(table),
                        HelperBase.classes[table].__table__)
            except Exception as e:
                self.logger.error(
                    "Error setting attribute for table: {} : {}".format(table, e))

        #looks for api keys one folder before the root augur folder.
        insert_sql_file(self.db, "../oauth.sql")

        self.logger.info("Trying to find max id of table...")
        try:
            self.history_id = self.get_max_id(
                'worker_history', 'history_id', operations_table=True) + 1
        except Exception as e:
            self.logger.info(f"Could not find max id. ERROR: {e}")
        
        # Organize different api keys/oauths available
        self.logger.info("Initializing API key.")
        if 'gh_api_key' in self.config or 'gitlab_api_key' in self.config:
            try:
                self.init_oauths(self.platform)
            except AttributeError:
                self.logger.error("Worker not configured to use API key!")
        else:
            self.oauths = [{'oauth_id': 0}]
