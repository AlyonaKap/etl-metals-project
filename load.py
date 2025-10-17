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

    for date, row in df.iterrows():
        date = date.strftime("%Y-%m-%d")
        data_dict = row.to_dict()
        for k, v in data_dict.items():
            if pd.isna(v):
                data_dict[k] = None
        source_ref.child(date).set(data_dict)
    return print(f"{name} data loaded to Firebase")
