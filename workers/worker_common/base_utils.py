import json
import sqlalchemy as s
import pandas as pd
import requests
import time
import datetime


"""
Things that might want to be encapsulated:

logger
db
headers
oauth
results counter for find_id_from_login
contributors
"""



def write_debug_data(data, name):
    """
        Write json data to file seperate from log files.

        :param data: json - json data that needs to be dumped to a file
        :param name: string - name of file to dump json data to
    """

    with open(f'{name}.json','w') as f:
        json.dump(data,f)

#This might be obsolete.
#Also I don't like passing in db and logger every damn function
def find_id_from_login(logger,db,login,headers,plaform='github'):
    """ Retrieves our contributor table primary key value for the contributor with
            the given GitHub login credentials, if this contributor is not there, then
            they get inserted.

        :param logger: The celery logger object.
        :param db: Database Engine, connection to augur postgres instance.
        :param login: String, the GitHub login username to find the primary key id for
        :param headers: Headers to make requests to github.
        :return: Integer, the id of the row in our database with the matching GitHub login
    """

    idSQL = s.sql.text("""
            SELECT cntrb_id FROM contributors WHERE cntrb_login = '{}' \
            AND LOWER(data_source) = '{} api'
            """.format(login, platform))
    
    rs = pd.read_sql(idSQL, db, params={})
    data_list = [list(row) for row in rs.itertuples(index=False)]

    try:
        return data_list[0][0]
    except:
        logger.info('contributor needs to be added...')
    

    if platform == 'github':
        cntrb_url = ("https://api.github.com/users/" + login)
    elif platform == 'gitlab':
        cntrb_url = ("https://gitlab.com/api/v4/users?username=" + login )

    logger.info("Hitting endpoint: {} ...\n".format(cntrb_url))

    while True:
        try:
            r = requests.get(url=cntrb_url, headers=headers)
            break
        except TimeoutError as e:
            logger.info("Request timed out. Sleeping 10 seconds and trying again...\n")
            time.sleep(30)

    #self.update_rate_limit(r)
    contributor = r.json()

    company = None
    location = None
    email = None
    if 'company' in contributor:
        company = contributor['company']
    if 'location' in contributor:
        location = contributor['location']
    if 'email' in contributor:
        email = contributor['email']
    

    if platform == 'github':
        cntrb = {
            'cntrb_login': contributor['login'] if 'login' in contributor else None,
            'cntrb_email': contributor['email'] if 'email' in contributor else None,
            'cntrb_company': contributor['company'] if 'company' in contributor else None,
            'cntrb_location': contributor['location'] if 'location' in contributor else None,
            'cntrb_created_at': contributor['created_at'] if 'created_at' in contributor else None,
            'cntrb_canonical': contributor['email'] if 'email' in contributor else None,
            'gh_user_id': contributor['id'] if 'id' in contributor else None,
            'gh_login': contributor['login'] if 'login' in contributor else None,
            'gh_url': contributor['url'] if 'url' in contributor else None,
            'gh_html_url': contributor['html_url'] if 'html_url' in contributor else None,
            'gh_node_id': contributor['node_id'] if 'node_id' in contributor else None,
            'gh_avatar_url': contributor['avatar_url'] if 'avatar_url' in contributor else None,
            'gh_gravatar_id': contributor['gravatar_id'] if 'gravatar_id' in contributor else None,
            'gh_followers_url': contributor['followers_url'] if 'followers_url' in contributor else None,
            'gh_following_url': contributor['following_url'] if 'following_url' in contributor else None,
            'gh_gists_url': contributor['gists_url'] if 'gists_url' in contributor else None,
            'gh_starred_url': contributor['starred_url'] if 'starred_url' in contributor else None,
            'gh_subscriptions_url': contributor['subscriptions_url'] if 'subscriptions_url' in contributor else None,
            'gh_organizations_url': contributor['organizations_url'] if 'organizations_url' in contributor else None,
            'gh_repos_url': contributor['repos_url'] if 'repos_url' in contributor else None,
            'gh_events_url': contributor['events_url'] if 'events_url' in contributor else None,
            'gh_received_events_url': contributor['received_events_url'] if 'received_events_url' in contributor else None,
            'gh_type': contributor['type'] if 'type' in contributor else None,
            'gh_site_admin': contributor['site_admin'] if 'site_admin' in contributor else None
        }
    elif platform == 'gitlab':
        cntrb =  {
            'cntrb_login': contributor[0]['username'] if 'username' in contributor[0] else None,
            'cntrb_email': email,
            'cntrb_company': company,
            'cntrb_location': location,
            'cntrb_created_at': contributor[0]['created_at'] if 'created_at' in contributor[0] else None,
            'cntrb_canonical': email,
            'gh_user_id': contributor[0]['id'],
            'gh_login': contributor[0]['username'],
            'gh_url': contributor[0]['web_url'],
            'gh_html_url': None,
            'gh_node_id': None,
            'gh_avatar_url': contributor[0]['avatar_url'],
            'gh_gravatar_id': None,
            'gh_followers_url': None,
            'gh_following_url': None,
            'gh_gists_url': None,
            'gh_starred_url': None,
            'gh_subscriptions_url': None,
            'gh_organizations_url': None,
            'gh_repos_url': None,
            'gh_events_url': None,
            'gh_received_events_url': None,
            'gh_type': None,
            'gh_site_admin': None
        }
    result = db.execute(self.contributors_table.insert().values(cntrb))
    logger.info("Primary key inserted into the contributors table: " + str(result.inserted_primary_key))
    self.results_counter += 1
    self.cntrb_id_inc = int(result.inserted_primary_key[0])
    self.logger.info(f"Inserted contributor: {cntrb['cntrb_login']}\n")