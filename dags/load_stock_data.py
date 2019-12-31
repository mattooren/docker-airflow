from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import date, datetime
from sqlalchemy import create_engine
from psycopg2.errors import UniqueViolation

AZURE_IP_ADDRESS = 'stocks-db.postgres.database.azure.com'
AZURE_PORT = '5432'
AZURE_USERNAME = 'mattooren@stocks-db'
AZURE_PASSWORD = 'YehNYA97vZGueESf'
AZURE_TABLE = 'postgres'


def parse_ASN_koersen_table(table):
    table_rows = table.find_all('tr')

    res = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [tr.text.strip() for tr in td if tr.text.strip()]
        if row:
            res.append(row)
        else:
            th = tr.find_all('th')
            columns = [tr.text.strip() for tr in th if tr.text.strip()]

    return pd.DataFrame(res, columns=columns)


def load_koersen_from_ASN(**kwargs):
    result = requests.get("https://www.asnbank.nl/zakelijk/zakelijke-beleggingsrekening/koersen.html")
    soup = BeautifulSoup(result.content, 'html.parser')
    table = soup.find(lambda tag: tag.name == 'table' and tag.has_attr('id') and tag['id'] == "table410710")
    df = parse_ASN_koersen_table(table)
    load_koersen_into_database(df)
    return df


def load_koersen_into_database(df_koersen):
    con = connect_to_database()
    add_funds(con, df_koersen.iloc[:,0].array)
    add_share_prices(con, df_koersen)


def add_funds(con, array_koersen):
    for koers in array_koersen:
        result = con.execute("SELECT * FROM fund WHERE fund_id = '{}'".format(koers))
        if result.rowcount == 0:
            con.execute("INSERT INTO fund(fund_id) VALUES ('{}') ".format(koers))

def add_share_prices(con, df_koersen):
    data_koersen = df_koersen.set_index(df_koersen.columns[0]).stack().reset_index()
    data_koersen.columns = ['fund_id', 'datetime', 'share_price']
    data_koersen['datetime'] = pd.to_datetime(data_koersen['datetime'], format='%d-%m-%Y')
    data_koersen['share_price'] = data_koersen['share_price'].apply(lambda x: x.replace(',' , '.')).astype(float)
    data_koersen['creation_time'] = datetime.now()

    current_id = con.execute("SELECT MAX(share_price_id) from share_price").first()[0]
    current_id = 0 if current_id == None else current_id + 1

    for index, row in data_koersen.iterrows():
        query = "INSERT INTO share_price(share_price_id, fund_id, datetime, share_price, creation_time) VALUES('{}', '{}', '{}', '{}', '{}')".format(
            current_id,
            row['fund_id'],
            row['datetime'],
            row['share_price'],
            row['creation_time']
        )

        result = con.execute("SELECT * FROM share_price WHERE fund_id = '{}' AND datetime = '{}'".format(row['fund_id'], row['datetime']))
        if result.rowcount == 0:
            con.execute(query)
        current_id += 1



def connect_to_database():
    url = 'postgresql://{}:{}@{}:{}/{}'.format(AZURE_USERNAME, AZURE_PASSWORD, AZURE_IP_ADDRESS, AZURE_PORT, AZURE_TABLE)
    engine = create_engine(url)
    return engine


if __name__ == '__main__':
    ds = 1
    print(load_koersen_from_ASN())