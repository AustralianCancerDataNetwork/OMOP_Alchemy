from pathlib import Path
import sqlalchemy as sa
import os, yaml, logging, keyring
from sqlalchemy.engine.url import URL
import multiprocessing as mp

max_depth = 3

try:
    CONFIG_PATH = Path(os.environ['OA_CONFIG'])
except KeyError:
    print('WARNING: Missing OA_CONFIG environment variable - we will try locate a config file but if not found, will return an empty config - this may have unintended consequences')
    CONFIG_PATH = Path(__file__).parent

config_filename = 'oa_system_config.yaml'

for _ in range(max_depth):
    for folder in CONFIG_PATH.iterdir():
        if (folder / config_filename).exists():
            CONFIG_PATH = CONFIG_PATH / folder
            break
    if not (CONFIG_PATH / config_filename).exists():
        CONFIG_PATH = CONFIG_PATH.parent


log_levels = {'info': logging.INFO,
              'debug': logging.DEBUG, 
              'warning': logging.WARNING, 
              'error': logging.ERROR,
              'critical': logging.CRITICAL}

def read_config_file(path):
    with open(path, 'r') as stream:
        try:
            file_content = yaml.safe_load(stream)
            return {k: Config(v) if isinstance(v, dict) else v for k, v in file_content.items()}
        except yaml.YAMLError as exc: #pragma: no cover
            raise exc

class Config(object):

    # stores system configuration information to setup and run CaVa
    # config can be accessed by attribute or subscript up to the 2nd
    # level (for convenience when using f strings) - i.e.
    # c['source']['platform'] == c.source.platform == c.source['platform']

    def __init__(self, dict_vals=None, config_path_override=''):
        if dict_vals == None:
            if config_path_override == '':
                self.config_path = CONFIG_PATH
            else:
                self.config_path = config_path_override # this usage is intended only for stubbing out test fixtures
            self.config_file = CONFIG_PATH / config_filename
            assert self.config_file.exists(), f'Missing config file: looking for {self.config_file}, cwd = {os.getcwd()}'
            self.settings = read_config_file(self.config_file)
            self.app_root = Path(self.filesystem.settings['app_root'])# Path(__file__).parent.parent
            self._engine = None

            for location in ['log_path', 'data_path']:
                self.settings[location] = self.app_root 
                for sub_folder in self.filesystem.settings[location]:
                    self.settings[location] = self.settings[location] / sub_folder
        else:
            self.config_file = ''
            self.settings = dict_vals
        try:
            self.log_level = log_levels[self.logging.loglevel]
        except KeyError:
            #self.log_level = log_levels['debug']
            ...

    def set_db_config(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.engine = self.get_engine('cdm')
        
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
        if k.callable():
            return self._call__(k)
        return self.settings.get(k, default)

    def save_config(self):
        yaml_data = {k:v.settings if hasattr(v, 'settings') else v for k, v in self.settings.items()}
        with open(self.config_file, 'w') as stream:
            try:
                file_content = yaml.safe_dump(yaml_data)
                stream.write(file_content)
            except yaml.YAMLError as exc: 
                raise exc
    
    def get_cnx_str(self, db, pwd=''):
        # returns raw connection string for database depending on 
        # configuration yaml and secret password stored in keyring 
        if pwd == '':
            pwd = self.get_db_pwd()
        db_config = {'database': db,
                    'drivername': self.db['platform'],
                    'username': self.db['db_uid_env'],
                    'password': pwd,
                    'host': self.db['host'],
                    'port': self.db['port'],
                    'query': {}
                    }
        local_data_path = self.data_path / self.filesystem.local_db
        if self.db['platform'] == 'sqlite':
            cnx_str = f'sqlite:///{local_data_path}'
        else:
            cnx_str = URL(**db_config)
        return cnx_str

    def get_db_pwd(self):
        try:
            with open(self.config_path / 'pwd_override.txt', 'r') as infile:
                pwd = infile.read()
        # retrieves database password from keyring according to parameters in configuration yaml
        except:
            try:
                service = self.db['db_pid_env']
                user = self.db['db_uid_env']
                pwd = keyring.get_password(service, user)
                if not pwd:
                    logging.warning(f'empty host password for user: {user}')
                return pwd
            except Exception as e:
                logging.error(f'{str(e)}')


    def get_engine(self, db, echo=False, pwd_override=''):
        # get database engine for either db='source' or db='target' according
        # to configuration yaml
        if self._engine:
            return self._engine
        cnx_str = self.get_cnx_str(db, pwd=pwd_override)
        try:
            cnx_args = {} if self.db['platform'] == 'sqlite' else {'connect_timeout': 10}
            self._engine = sa.create_engine(cnx_str, echo=echo, connect_args=cnx_args)
            return self._engine
        except Exception as e:
            logging.error(f'Timeout for database connection: {cnx_str}')
            raise(e)


oa_config = Config()
oa_config.set_db_config()

if oa_config.logging.log_target == 'file':
    oa_config.log_path.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(filename=oa_config.log_path / oa_config.filesystem.log_file)
else:
    handler = logging.StreamHandler()

logging.basicConfig(level=log_levels[oa_config.logging.log_level], 
                    format='%(name)s - %(levelname)s - %(message)s',
                    handlers=[handler]
                    )
logger = logging.getLogger(__name__)
logger.debug(f'Application config path set: {oa_config.config_file}')
logger.debug(f'Log path set: {oa_config.settings["log_path"]}')
logger.debug(f'DB connection string: {oa_config.get_cnx_str("cdm")}')