import os
from dotenv import load_dotenv

load_dotenv()

url = f"jdbc:postgresql://{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"

properties = {
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "driver": "org.postgresql.Driver"
}

print("DB_HOST:", os.getenv("DB_HOST"))
print("DB_PORT:", os.getenv("DB_PORT"))
print("DB_NAME:", os.getenv("DB_NAME"))
print("DB_USER:", os.getenv("DB_USER"))
print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))