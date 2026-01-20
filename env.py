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
