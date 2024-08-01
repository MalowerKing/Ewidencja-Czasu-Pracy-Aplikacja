from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from TuyaDeviceProvider import *
from datetime import datetime
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
    # Grupowanie po dacie
    date_groups = defaultdict(list)
    for entry in data:
        date_groups[entry['date']].append(entry)

    # Filtrownanie aby pozostały tylko najniższe i najwyższe dane
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
    # Ping Bazę danych
    client.admin.command('ping')
except: # W przypadku błędu połączenia
    print("Błąd w połączeniu z bazą danych, albo z TuyaAPI")

if __name__ == '__main__':
    # Zaciągnięcie logów z Tuya Api
    logs = device_provider.getDeviceLog()
    
    #Kolekcje Bazy Danych
    myPracownicy =  client["pracownicy"]
    myDB =  client["czas_pracy"]
    
    #Pętla po każdym pracwniku w bazie Pracowników ID
    for pracownik in myPracownicy['PracownicyID'].find({},{"_id": 0,"FingerID":1, "cardID":1,"imie":1,"nazwisko":1}):
        #Szablony potrzebynych danych oraz 
        temp_insert_list = []
        insertList = []
        collection_name = pracownik["imie"]+'_'+pracownik['nazwisko']
        print(collection_name)
        temp_dict = {
            "date":"",
            "time":""
        }
        entrence_time = None
        exit_time = None
        #Iterowanie po logach, tworzenie pomocniczej tabeli 
        for log in logs:
            if log['value'] == pracownik['cardID'] or log['value'] == pracownik['FingerID']: # Znajdowanie w logach pasujacych kodów odcisków
                temp_dict = {
                    "date": log['date'],
                    "time": log['time'],
                }
                temp_insert_list.append(temp_dict)
        temp_insert_list.sort(key=lambda x: (x['date'], x['time'])) # Posortowanie pozostałych


        for element in temp_insert_list: # Iterowanie po elementach listy
            if entrence_time is None: # Pierwszy rekord danego dnia to wejscie
                entrence_time = element['time']
            else: # Drugi to wyjscie
                login_gap = (datetime.strptime(element['time'], '%H:%M:%S') - datetime.strptime(entrence_time, '%H:%M:%S')).total_seconds() / 60.0 # login_gap w minutach
                if login_gap > 15:
                    exit_time = element['time']
                    hours_worked = calculate_hours(entrence_time, exit_time) # Liczenie przepracownych godzin

                    # Uzupełenianie poprawnego wpisu do bazy
                    log_entry = {
                        "date": element['date'],
                        "entrence_time": entrence_time,
                        "exit_time": exit_time,
                        "hours": hours_worked
                    }
                    # Dodanie do listy wpisów
                    insertList.append(log_entry)
                    
                    # Wyczyszczenie danych przed następną pętlą
                    entrence_time = None
                    exit_time = None
                else:
                    # Pomiń exit_time
                    entrence_time = element['time'] # Ustaw nowe entrence_time, jako czas ostatniego pominiętego wpisu


        if len(insertList) != 0: # Sprawdzanie czy lista nie jest pusta
            myDB[collection_name].insert_many(insertList) # Wpis do bazy

