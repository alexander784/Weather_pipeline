import requests


api_url = "https://api.weatherstack.com/current?access_key=bb&query=Nairobi"

def fetch_data():
    print("Fetching weather data from weather stack....")
    try:

        res = requests.get(api_url)
        res.raise_for_status()
        print("Api res received successfully.")
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occured: {e}")
        raise

# fetch_data()






 