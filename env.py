import dotenv
import os

dotenv.load_dotenv()

env = os.getenv("ENV")
TOKEN = os.getenv("TOKEN")
BARK_KEY = os.getenv("BARK_KEY")

FS_APP_ID = os.getenv("FS_APP_ID")
FS_APP_SECRET = os.getenv("FS_APP_SECRET")

FS_FOLDER_TOKEN = os.getenv("FS_FOLDER_TOKEN")
FS_APP_TOKEN = os.getenv("FS_APP_TOKEN")
FS_TABLE_ID = os.getenv("FS_TABLE_ID")

__proxy_url = "http://%(user)s:%(password)s@%(server)s" % {
    "user": os.getenv("proxy_key"),
    "password": os.getenv("proxy_secret"),
    "server": os.getenv("proxy_server"),
}
proxy_conf = {
    'http': __proxy_url,
    'https': __proxy_url,
} if __proxy_url else None
