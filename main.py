from  pymongo import MongoClient
from pymongo.server_api import ServerApi
import pymongo
from TuyaDeviceProvider import *
from datetime import datetime, date
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
    current_time = date.today()

    #Kolekcje Bazy Danych
    myPracownicy =  client["pracownicy"]
    myDB =  client["czas_pracy"]

    insertActiveUsers = []


    #Pętla po każdym pracwniku w bazie Pracowników ID
    for pracownik in myPracownicy['PracownicyID'].find({},{"_id": 0,"FingerID":1, "cardID":1,"imie":1,"nazwisko":1}):
        Active = False
        #Szablony potrzebynych danych oraz
        temp_insert_list = []
        insertList = []
        collection_name = pracownik["imie"]+'_'+pracownik['nazwisko']
        print(collection_name)
        temp_dict = {
            "date":"",
            "time":""
        }

        bulkWrite = []
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

        index = 0
        for element in temp_insert_list: # Iterowanie po elementach listy
            index += 1 # index sprawdzający czy yo nie jest ostatni element w tablicy
            if entrence_time is None: # Pierwszy rekord danego dnia to wejscie
                entrence_time = element
                if index == len(temp_insert_list):
                    # Uzupełenianie poprawnego wpisu do bazy) #Sprawdzanie ostatniego wpisu
                    if str(entrence_time['date']) == str(current_time): #Pracownik jest obecny jeżeli data jest dzisiejsza
                        log_entry = {
                            "date": element['date'],
                            "entrence_time": entrence_time['time'],
                            "exit_time": "Obecny",
                            "hours": 0
                        }
                    else: #Albo ma nie poprawne odbicie
                        log_entry = {
                            "date": element['date'],
                            "entrence_time": entrence_time['time'],
                            "exit_time": "Nie poprawne odbicie",
                            "hours": 0
                        }
                    # Dodanie do listy wpisów
                    insertList.append(log_entry)

                    # Wyczyszczenie danych przed następną pętlą
                    entrence_time = None
                    exit_time = None

            else: # Drugi to wyjscie
                login_gap = (datetime.strptime(str(element['time']), '%H:%M:%S') - datetime.strptime(entrence_time['time'], '%H:%M:%S')).total_seconds() / 60.0 # login_gap w minutach
                if element['date'] != entrence_time['date']: #Sprawdzenie czy zostało poprawnie odbita karta na wyjściu
                    log_entry = {
                        "date": entrence_time['date'],
                        "entrence_time": entrence_time['time'],
                        "exit_time": "Nie poprawne odbicie",
                        "hours": 0
                    }
                    insertList.append(log_entry)
                    entrence_time = element
                elif login_gap > 15: # Sprawdzanie czy nie jest to pomyłka przy odbiciu
                    exit_time = element['time']
                    hours_worked = calculate_hours(entrence_time['time'], exit_time) # Liczenie przepracownych godzin

                    # Uzupełenianie poprawnego wpisu do bazy
                    log_entry = {
                        "date": element['date'],
                        "entrence_time": entrence_time['time'],
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
                    pass # Ustaw nowe entrence_time, jako czas ostatniego pominiętego wpisu

        if len(insertList) != 0: # Sprawdzanie czy lista nie jest pusta
            # myDB[collection_name].insert_many(insertList) # Wpis do bazy
            for insert in insertList: # tworzenie Query writeBulk
                temp_insert = pymongo.UpdateOne({
                        'date' : insert['date'],
                        'entrence_time' : insert['entrence_time']},
                    { "$set" : {
                        "date": insert['date'],
                        "entrence_time": insert['entrence_time'],
                        "exit_time": insert['exit_time'],
                        "hours": insert['hours'] }
                    }, True)
                bulkWrite.append(temp_insert)
            for element in insertList:
                if element['exit_time'] == "Obecny":
                    temp_insert = pymongo.UpdateOne({
                        'imie' : pracownik['imie'],
                        'nazwisko' : pracownik['nazwisko']},
                    { "$set" : {
                        "active": "Obecny"}
                    })
                    insertActiveUsers.append(temp_insert)
                    Active = True
                    break
            if Active == False:
                temp_insert = pymongo.UpdateOne({
                    'imie' : pracownik['imie'],
                    'nazwisko' : pracownik['nazwisko']},
                { "$set" : {
                    "active": "NieObecny"}
                })
                insertActiveUsers.append(temp_insert)
            print(myDB[collection_name].bulk_write(bulkWrite, False) )#Zapis do Bazy danych CzasPracy
    print(myPracownicy["PracownicyID"].bulk_write(insertActiveUsers, False))
