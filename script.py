import os
import sys
import dotenv
import requests
import json
from time import sleep
dotenv.load_dotenv()

KEY = os.getenv("EXPORT_COMMENTS_KEY")

HEADERS = {
    "X-AUTH-TOKEN": KEY,
    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
}

BASE_URL = "https://exportcomments.com"
API_URL = f"{BASE_URL}/api/v2"


def job_response(guid: str) -> dict:
    """The exported data for the job in a dictionary."""
    
    response = requests.get(
        f"{API_URL}/export", 
        headers=HEADERS, 
        params={"guid": guid}, 
        timeout=60
        )
    return response.json()


def job_status(guid: str) -> str:
    """Checks status of the job with the given guid"""

    response = job_response(guid)
    data = response["data"]
    
    try:
        return data[0]["status"]
    except KeyError:
        raise ValueError(f"'status' or index 0 not found in data: {data}")


def get_response(guid: str):
    """Gets the response for the job with the given guid."""
    
    while True:
        status = job_status(guid)
    
        if status == 'done':
            break
        elif status == 'error':
            print(f"Error processing job {guid}. Status: {status}")
            error = job_response(guid)["data"][0]["error"]
            print(f"Error: {error}")
            break
            #sys.exit()

        print(f"Status: {status}. Waiting 20 seconds to check again.")
        sleep(20)

def raw_url(guid: str) -> str:
    """Gets the url for the download of the raw exported data.
    This reponse is relative to a base URL"""

    response = job_response(guid)
    data = response["data"]
    
    try:
        return data[0]["rawUrl"]
    except KeyError:
        raise ValueError(f"'rawUrl' or index 0 not found in data: {data}")


def download_raw(raw_url: str) -> dict:
    """Downloads the raw results as JSON from the given URL"""

    response = requests.get(f"{BASE_URL}{raw_url}", headers=HEADERS, timeout=60)

    if response.status_code == 200:
        return response.json()
    raise ValueError(f"[FAILED TO DOWNLOAD] Status Code: {response.status_code}")


def write_raw_to_file(raw_data: dict, filename: str):
    """Writes the raw data to a file in JSON format.
    """
    with open(filename, "w") as file:
        json.dump(raw_data, file)


def start_job(url: str, max_retries=3) -> str:
    """Starts a job to export comments from the given URL."""

    paramns = {
        "url": url,
        "options": json.dumps({
            #"sessionid": "",
            
            # É possível adicionar mais opções de extração aqui
        })
    }

    for _ in range(max_retries):
        response = requests.put(
            f"{API_URL}/export",
            headers=HEADERS,
            timeout=30,
            params=paramns
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to start job: {response.text}")
        
        try:
            response = response.json()
            status_code = response["data"].get("status_code")
            
            if status_code == 429: # Rate limit exceded
                seconds_to_wait = response["data"]["seconds_to_wait"]
                print(f"Rate limit exceeded. Waiting {seconds_to_wait} seconds.")
                sleep(seconds_to_wait)
                continue # Retry
                
            guid = response["data"]["guid"]
            job_status = response["data"]["status"]
            print(f"Started job {guid} Status: {job_status}")
            return guid
        except Exception as e:
            raise Exception(f"Failed to start job: {response}")


if __name__ == '__main__':
    urls = sys.argv[1:]
    
    for url in urls:
        guid = start_job(url)
        get_response(guid)
        raw = raw_url(guid)
        raw_data = download_raw(raw)
        write_raw_to_file(raw_data, f"{guid}.json")
        print(f"Data written to {guid}.json")
        