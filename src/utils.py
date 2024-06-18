from pyspark.sql import SparkSession
import requests

# Third parties import :
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


def get_secret_var(spark: SparkSession, secret_scope: str, secret_key: str):
    return get_dbutils(spark).secrets.get(scope=secret_scope, key=secret_key)


def get_crypto_rates(currency):
    # Using CoinGecko API to get current prices
    url = f'https://api.coingecko.com/api/v3/simple/price?ids={currency}&vs_currencies=usd'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if currency in data and 'usd' in data[currency]:
            usd_price = data[currency]['usd']
            return usd_price
        else:
            return None
    else:
        print(f"Error fetching data from CoinGecko API: {response.status_code}")
        return None
    
def get_add_details(street_address: str) -> 'tuple[str, str, str]':
    # Get the city & costal pode & country from street address :  
    geolocator = Nominatim(user_agent="MyGeocodingApp/1.0 (mallouk@gmail.com)") 
    location = geolocator.geocode(street_address, timeout=10)
    
    if location : 
        return str(location).split(', ')[-3:] # City, Postal Code, Country
    
    else : 
        return "Unknown", "Unknown", "Unknown"  