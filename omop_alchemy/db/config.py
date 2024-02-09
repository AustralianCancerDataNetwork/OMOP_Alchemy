from pathlib import Path
import sqlalchemy as sa
import os, yaml, sys, logging, keyring

max_depth = 3

try:
    CONFIG_PATH = Path(os.environ['OA_CONFIG'])
except KeyError:
    print('WARNING: Missing OA_CONFIG environment variable')
    CONFIG_PATH = Path(os.getcwd())

config_filename = 'oa_system_config.yaml'

for _ in range(max_depth):
    if not (CONFIG_PATH / config_filename).exists():
        CONFIG_PATH = CONFIG_PATH.parent


# TODO make this more flexible so that you aren't stuck with sqlite demo only

class Config(object):

    # stores system configuration information to setup and run CaVa
    # config can be accessed by attribute or subscript up to the 2nd
    # level (for convenience when using f strings) - i.e.
    # c['source']['platform'] == c.source.platform == c.source['platform']

    def __init__(self, dict_vals=None):
        log_levels = {'info': logging.INFO,
                      'debug': logging.DEBUG, 
                      'warning': logging.WARNING, 
                      'error': logging.ERROR,
                      'critical': logging.CRITICAL}
        if dict_vals == None:
            self.config_path = CONFIG_PATH
            self.config_file = config_filename
            assert self.config_file.exists(), f'Missing config file: looking for {self.config_file}, cwd = {os.getcwd()}'
            self.settings = read_config_file(self.config_file)
            self.app_root = Path(__file__).parent.parent
            for location in ['log_path', 'data_path']:
                self.settings[location] = Path(self.filesystem.data_root) 
                for sub_folder in self.filesystem.settings[location]:
                    self.settings[location] = self.settings[location] / sub_folder
        else:
            self.config_file = ''
            self.settings = dict_vals
        try:
            self.log_level = log_levels[self.logging.loglevel]
        except KeyError:
            self.log_level = log_levels['debug']
        
    def set_db_config(self):
        ...
        #         if self.filesystem.local_db:
        #     self.db_dir = self.data_path / self.filesystem.local_db
        #     self.DATABASE_URI = f'sqlite:///{db_dir.resolve()}'
        # else:


        # db_dir = data_path / 'dash.db'
        # vocab_load = data_path 

        # vocab_load.mkdir(parents=True, exist_ok=True)

        # VOCAB_PATH = f'{vocab_load.resolve()}'
        # SECRET_KEY='dev'

    def keys(self):
        return self.settings.keys()

    def items(self):
        return self.settings.items()

    def __getitem__(self, x):
        if x == '':
            return self
        return getattr(self, x)

    def __getattr__(self, k):
        if k == '':
            return self
        return self.settings[k]

    def update_item(self, k, value):
        self.settings[k] = value

    def get(self, k, default=None):
        return self.settings.get(k, default)

    def save_config(self):
        yaml_data = {k:v.settings if hasattr(v, 'settings') else v for k, v in self.settings.items()}
        with open(self.config_file, 'w') as stream:
            try:
                file_content = yaml.safe_dump(yaml_data)
                stream.write(file_content)
            except yaml.YAMLError as exc: #pragma: no cover
                raise exc

    def list_databases(self):
        return {label: get_db_details(self[label]) for label in ['db']}

def get_db_details(db_dict):
    try:
        return {key: {label: d[label] for label in ['schema', 'host', 'platform', 'port']} for key, d in db_dict.items()}
    except:
        return 'invalid database config'


def read_config_file(path):
    with open(path, 'r') as stream:
        try:
            file_content = yaml.safe_load(stream)
            return {k: Config(v) if isinstance(v, dict) else v for k, v in file_content.items()}
        except yaml.YAMLError as exc: #pragma: no cover
            raise exc


oa_config = Config()
#engine = sa.create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False)


def get_db_pwd(location, schema=''):
    # retrieves database password from keyring according to parameters in configuration yaml
    try:
        service = oa_config[location][schema]['db_pid_env']
        user = oa_config[location][schema]['db_uid_env']
        pwd = keyring.get_password(service, user)
        if not pwd:
            obj.cava_log.log(f'empty host password for user: {user}', 'warning')
        return pwd
    except Exception as e:
        obj.cava_log.log(f'{str(e)}', 'error')


def get_engine(db, schema='', echo=False, pwd_override=''):
    # get database engine for either db='source' or db='target' according
    # to configuration yaml
    cnx_str = get_cnx_str(db, pwd=pwd_override, schema=schema)
    return sa.create_engine(cnx_str, echo=echo)

def get_cnx_str(db, pwd='', schema=''):
    # returns raw connection string for database depending on 
    # configuration yaml and secret password stored in keyring 
    if pwd == '':
        pwd = get_db_pwd(db, schema)
    if schema == '':
        dets = obj.cava_config[db]
    else:
        dets = obj.cava_config[db][schema]
    db_config = {'drivername': dets['platform'],
                 'username': dets['db_uid_env'],
                 'password': pwd,
                 'host': dets['host'],
                 'port': dets['port']}
    local_data_path = obj.cava_config.filesystem.local_data
    if dets['platform'] == 'sqlite':
        cnx_str = f'sqlite:///{local_data_path}_{db}'
    else:
        cnx_str = URL(**db_config)
    return cnx_str
