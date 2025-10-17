import os
from dotenv import load_dotenv

import pandas as pd

import firebase_admin
from firebase_admin import credentials, db

load_dotenv()
db_url = os.getenv("FIREBASE_URL")


def load_to_csv(df, name, out_dir="csv"):
    os.makedirs(out_dir, exist_ok=True)
    csv_name = os.path.join(out_dir, f"{name}.csv")
    df.to_csv(csv_name, index=True)
    return print(f"{name} data saved to {csv_name}")


def connect_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate("credentials.json")
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": db_url},
        )
    return db.reference("/")


def load_to_firebase(df, name):
    ref = connect_firebase()
    source_ref = ref.child(name)
    data_to_update = {}

    for date, row in df.iterrows():
        date = date.strftime("%Y-%m-%d")
        data_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        data_to_update[date] = data_dict
        
    source_ref.update(data_to_update)
    return print(f"{name} data loaded to Firebase")
