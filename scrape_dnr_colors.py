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

    #with open('park_metadata.pickle', 'rb') as metadata_file:
    #    park_metadata = pickle.load(metadata_file)

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

    response = requests.get(url)
    page = response.text

    soup = BeautifulSoup(page, 'html.parser')

    park_names = {park['name']: park['park_id'] for park in park_metadata}

    # The below is UNTESTED because the page contains JavaScript-rendered content.
    # Must render the page to get final HTML. Options:
    # - Requests-HTML: https://requests.readthedocs.io/projects/requests-html/en/latest/
    # - Playwright: https://github.com/microsoft/playwright-python
    # - Selenium: see this post https://stackoverflow.com/questions/8049520/how-can-i-scrape-a-page-with-dynamic-content-created-by-javascript-in-python

    # get table
    table = soup.find(id='summary_table').tbody
    # for row in table
    for row in table.children:
        # get location cell value
        table_park_name = str(row[1].a.contents)
        # if location in park_names
        if table_park_name in park_names.keys():
            # read that row's color cell value
            color_cell = row[0]
            # get the string
            contents_list = color_cell.stripped_strings
            # insert to appropriate park dict in today_data list. ensure is string, maybe by unicode(). make sure to trim sides of whitespace
            park_id = park_names[table_park_name]
            for park in today_data:
                if park['park_id'] == park_id:
                    park['fall_colors'] = contents_list
                    break


        #with open('today_data.pickle', 'wb') as today_file:
        #    pickle.dump(today_data, today_file)

        #with open('tomorrow_data.pickle', 'wb') as tomorrow_file:
        #    pickle.dump(tomorrow_data, tomorrow_file)