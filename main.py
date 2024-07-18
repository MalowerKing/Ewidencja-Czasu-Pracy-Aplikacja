from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from TuyaDeviceProvider import *
from datetime import datetime, time, timedelta
from collections import defaultdict
import Credentials

# Funkcja licząca ilość przepracowanych godzin
def calculate_hours(entrence_time, exit_time):
    fmt = '%H:%M:%S'  #Format daty
    #Ekstrakcja daty
    entrence_time_obj = datetime.strptime(entrence_time, fmt)  
    exit_time_obj = datetime.strptime(exit_time, fmt)
    hours_worked = (exit_time_obj - entrence_time_obj).total_seconds() / 3600.0  # convert seconds to hours
    return round(hours_worked, 2)

def filter_highest_lowest_times(data):
    # Step 1: Group by date
    date_groups = defaultdict(list)
    for entry in data:
        date_groups[entry['date']].append(entry)

    # Step 2: Filter to keep only highest and lowest time for each date
    result = []
    for date, entries in date_groups.items():
        # Sort entries by time
        sorted_entries = sorted(entries, key=lambda x: x['time'])
        # Keep the first (smallest time) and the last (highest time)
        result.append(sorted_entries[0])
        result.append(sorted_entries[-1])
    
    return result
#Stałe, link do bazy danych, szablon wstawionego elementu
uri = Credentials.Credentials.DataBase
insert_element = {
    "date": "",
    "entrence_time": "",
    "exit_time": "",
    "hours": "",
}
try:
    # Tworzenie kilenta Bazy danych
    client = MongoClient(uri, server_api=ServerApi('1'))
    #Tworzenie obiektu TuyaApi
    device_provider = TuyaDeviceLogProvider(Credentials.Credentials.TuyaDevice1)
except:
    print("Błąd w połączeniu z bazą danych, albo z TuyaAPI")

# Send a ping to confirm a successful connection
if __name__ == '__main__':
    logs = device_provider.getDeviceLog()
    client.admin.command('ping')
    myPracownicy =  client["pracownicy"]
    myDB =  client["czas_pracy"]
    
    for pracownik in myPracownicy['PracownicyID'].find({},{"_id": 0,"FingerID":1, "cardID":1,"imie":1,"nazwisko":1}):
        temp_insert_list = []
        insertList = []
        collection_name = pracownik["imie"]+'_'+pracownik['nazwisko']
        print(collection_name)
        temp_dict = {
            "date":"",
            "time":""
        }
        for log in logs:
            if log['value'] == pracownik['cardID'] or log['value'] == pracownik['FingerID']:
                temp_dict = {
                    "date": log['date'],
                    "time": log['time'],
                }
                temp_insert_list.append(temp_dict)
        temp_insert_list = filter_highest_lowest_times(temp_insert_list)
        temp_insert_list.sort(key=lambda x: (x['date'], x['time']))
        for element in temp_insert_list:
            print(element)
        iteratorDate = None
        entrence_time = None
        exit_time = None

        for element in temp_insert_list:
            if entrence_time is None: 
                entrence_time = element['time']
            else:
                exit_time = element['time']
                hours_worked = calculate_hours(entrence_time, exit_time)  # assuming this function is defined

                # Create a dictionary for the log entry
                log_entry = {
                    "date": element['date'],
                    "entrence_time": entrence_time,
                    "exit_time": exit_time,
                    "hours": hours_worked
                }
                insertList.append(log_entry)

                entrence_time = None
                exit_time = None

        for element in insertList:
            print(element)
            print("\n")
        if len(insertList) != 0:
            myDB[collection_name].insert_many(insertList)

