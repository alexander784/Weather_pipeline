import requests


api_url = "https://api.weatherstack.com/current?access_key=b6d4ec476bcfc235e0fa36b4366197e2&query=Nairobi"

def fetch_data():
    print("Fetching weather data from weather stack....")
    try:

        res = requests.get(api_url)
        res.raise_for_status()
        print("Api res received successgully.")
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occured: {e}")
        raise

# fetch_data()






 