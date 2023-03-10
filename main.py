from typing import Union
import urllib.parse as up
import psycopg2
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
import json
import os
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import pandas as pd

cur = None
conn = None
app = FastAPI()
origins = [
    "http://localhost:3000", "https://market-gui.vercel.app", "market-1a194uks2-rakshith-u-r.vercel.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    load_dotenv()
    global conn
    up.uses_netloc.append("postgres")
    url = up.urlparse(
        "postgres://xfelfohc:F-fp4eg_sXBTG8evRgiYoIyABFX8y1UY@tiny.db.elephantsql.com/xfelfohc")
    conn = psycopg2.connect(database=url.path[1:], user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port
                            )
    global cur
    cur = conn.cursor()


@app.on_event("shutdown")
async def shutdown():
    await conn.commit()
    await conn.close()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/ping")
def ping():
    return {"Ping": "Pong"}

def data_processing(data, head):
    data = pd.DataFrame.from_records(data)
    data.columns = head
    column_medians = data.median()
    data = data.fillna(column_medians)
    return data


def score_calc(data):
    head = ['market_id', 'sold_homes_count', 'new_listings_count',
            'homes_sold_over_list_price_count', 'median_sale_to_list_ratio', 'days_to_sell']
    data = data_processing(data, head)
    score = []
    for row in data.index:

        sold_homes_count = data['sold_homes_count'][row]
        new_listings_count = data['new_listings_count'][row]
        homes_sold_over_list_price_count = data['homes_sold_over_list_price_count'][row]
        median_sale_to_list_ratio = data['median_sale_to_list_ratio'][row]
        days_to_sell = data['days_to_sell'][row]

        if new_listings_count != 0 and sold_homes_count != 0 and homes_sold_over_list_price_count != 0 and days_to_sell != 0:

            hotness_score = (sold_homes_count / new_listings_count) * (homes_sold_over_list_price_count /
                                                                       sold_homes_count) * (1 / days_to_sell) * 1000000

            score.append(hotness_score)
    score = sum(score)/len(score)

    return score


def hotness_calc(data):
    head = ['market_id', 'sold_homes_count', 'new_listings_count',
            'homes_sold_over_list_price_count', 'median_sale_to_list_ratio', 'days_to_sell']
    data = data_processing(data, head)

    hot_list = {}
    for row in data.index:

        sold_homes_count = data['sold_homes_count'][row]
        new_listings_count = data['new_listings_count'][row]
        homes_sold_over_list_price_count = data['homes_sold_over_list_price_count'][row]
        median_sale_to_list_ratio = data['median_sale_to_list_ratio'][row]
        days_to_sell = data['days_to_sell'][row]

        if new_listings_count != 0 and sold_homes_count != 0 and homes_sold_over_list_price_count != 0 and days_to_sell != 0:

            hotness_score = (sold_homes_count / new_listings_count) * (homes_sold_over_list_price_count /
                                                                       sold_homes_count) * (1 / days_to_sell)*1000000
            hot_list.setdefault(str(data['market_id'][row]),
                                []).append(hotness_score)
    for key in hot_list:
        hot_list[key] = sum(hot_list[key]) / len(hot_list[key])
    # print(hot_list)
    return hot_list


@app.get("/all")
def hotness():
    # table_name = os.environ.get('METRIC')
    up.uses_netloc.append("postgres")
    url = up.urlparse(
        "postgres://xfelfohc:F-fp4eg_sXBTG8evRgiYoIyABFX8y1UY@tiny.db.elephantsql.com/xfelfohc")
    conn = psycopg2.connect(database=url.path[1:], user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port
                            )
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT * FROM market_hotness')
        rows = cur.fetchall()
        result = dict(rows)
        conn.close()
        return result
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/market')
def get_score(market_id: int):
    # table_name = os.environ.get('METRIC')
    up.uses_netloc.append("postgres")
    url = up.urlparse(
        "postgres://xfelfohc:F-fp4eg_sXBTG8evRgiYoIyABFX8y1UY@tiny.db.elephantsql.com/xfelfohc")
    conn = psycopg2.connect(database=url.path[1:], user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port
                            )
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT "market_hotness" from market_hotness where market_id = {market_id}')
        rows = cur.fetchall()

        if len(rows) == 0:
            conn.close()
            raise HTTPException(status=404, details="Market not found")
        conn.close()
        return {

            "market_id": rows[0][0]
        }
    except Exception as e:
        conn.close()
        return JSONResponse(status_code=404)

@app.get('/top5')
def get_top5():
    # table_name = os.environ.get('METRIC')
    up.uses_netloc.append("postgres")
    url = up.urlparse(
        "postgres://xfelfohc:F-fp4eg_sXBTG8evRgiYoIyABFX8y1UY@tiny.db.elephantsql.com/xfelfohc")
    conn = psycopg2.connect(database=url.path[1:], user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port
                            )
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT * FROM market_hotness order by "market_hotness" desc limit 5')
        rows = cur.fetchall()
        result = dict(rows)
        conn.close()
        return result
    except Exception as e:
        conn.close()
        return JSONResponse(status_code=404)


def history_calc(data):
    head = ["market_id", "time_bucket", "new_listings_count", "sold_homes_count", "homes_sold_over_list_price_count", "median_list_price_psqft", "median_sale_price_psqft", "median_sale_price", "median_sale_to_list_ratio", "days_to_pending", "days_to_sell"]
    data = data_processing(data, head)

    hot_list = []
    for row in data.index:

        sold_homes_count = data['sold_homes_count'][row]
        new_listings_count = data['new_listings_count'][row]
        homes_sold_over_list_price_count = data['homes_sold_over_list_price_count'][row]
        days_to_sell = data['days_to_sell'][row]

        if new_listings_count != 0 and sold_homes_count != 0 and homes_sold_over_list_price_count != 0 and days_to_sell != 0:

            hotness_score = (sold_homes_count / new_listings_count) * (homes_sold_over_list_price_count /
                                                                       sold_homes_count) * (1 / days_to_sell)*1000000
            hot_list.append([data["time_bucket"][row],hotness_score])
    
    return sorted(hot_list)


@app.get('/history')
def get_history(market_id: int):
    up.uses_netloc.append("postgres")
    url = up.urlparse(
        "postgres://xfelfohc:F-fp4eg_sXBTG8evRgiYoIyABFX8y1UY@tiny.db.elephantsql.com/xfelfohc")
    conn = psycopg2.connect(database=url.path[1:], user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port
                            )
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT * from market_metrics where market_id = {market_id}')
        rows = cur.fetchall()

        if len(rows) == 0:
            conn.close()
            raise HTTPException(status=404, details="Market not found")
        conn.close()
        return dict(history_calc(rows))
    except Exception as e:
        conn.close()
        return JSONResponse(status_code=404)