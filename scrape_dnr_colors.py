from datetime import date

# run only from Sept 1 through mid-November
today = date.today()
this_year_start = today.replace(month=9, day=1)
this_year_end =  today.replace(month=11, day=14)

if today >= this_year_start and today <= this_year_end:
    from bs4 import BeautifulSoup
    #import pickle
    import requests

    #with open('today_data.pickle', 'rb') as today_file:
    #    today_data = pickle.load(today_file)

    #with open('tomorrow_data.pickle', 'rb') as tomorrow_file:
    #    tomorrow_data = pickle.load(tomorrow_file)

    url = "https://www.dnr.state.mn.us/fall_colors/index.html"

    """
    try:
        response = requests.get(url, timeout=3.1)
        response.raise_for_status()
    except Timeout:
        pass
    except ConnectionError:
        pass
    except HTTPError:
        pass

    page = response.text

    soup = BeautifulSoup(page)
    """



    #with open('today_data.pickle', 'wb') as today_file:
    #    pickle.dump(today_data, today_file)

    #with open('tomorrow_data.pickle', 'wb') as tomorrow_file:
    #    pickle.dump(tomorrow_data, tomorrow_file)