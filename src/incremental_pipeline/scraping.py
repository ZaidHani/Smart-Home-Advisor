from bs4 import BeautifulSoup
import requests
import os
import glob
import re
import pandas as pd
import time
import datetime

def scrape_links(URL:str, pages:int) -> str:
    """Scrape product URLs from OpenSooq
    
    This function scrapes URLs for products listed on the specified number of pages from an OpenSooq category landing page.
    Each page typically contains 30 items. To avoid duplicates, use a URL that lists the most recent products.
    
    Parameters
    ----------
    url : str
        The category landing page URL you want to extract its products links, the URL must be raw meaning it must be devoid of query parameters.
    pages : int
        The number of pages to scrape.

    Returns
    -------
    str
        A message indicating the scraping process is complete.

    The function generates a CSV file 'data/links.csv' containing the scraped product links, IDs, and prices.
    
    Example
    -------
    url = 'https://jo.opensooq.com/en/real-estate-for-sale/all?search=true&sort_code=recent'
    scrape_links(url, 1122)
    """
    df = []
    for i in range(1, pages+1):
        url = f'{URL}&page={i}'
        r = requests.get(url) 
        soup = BeautifulSoup(r.content, features='html.parser')
        table = soup.find('div', attrs = {'id':'serpMainContent'})
        for row in table.findAll('a', {'class':re.compile('sc-e7fc5d43-0 gWwdCb postItem flex flexWrap mb-32 relative radius-8 grayHoverBg whiteBg boxShadow2 blackColor p-16')}):
            data = {}
            data['id'] = row['href'][11:20]
            data['link'] = 'https://opensooq.com'+row['href']
            data['price'] = row.find('div',{'class':'priceColor bold alignSelfCenter font-18 ms-auto'}).text
            df.append(data)
        url = URL
        print(f'Pages Scrpaed: {i}')
    pd.DataFrame(df).to_csv('data/incremental/links.csv',index_label=False,mode='w')
    return "Scraping Products Links is Done!"

def safe_extract(find_function, default=None):
    """Safely extracts a value using the provided function, returning a default value if an exception occurs.

    This helper function attempts to execute the `find_function` callable. If `find_function`
    raises an AttributeError or TypeError, the function catches the exception and returns the
    specified default value instead.

    Args:
        find_function (callable): A function that performs the desired extraction or operation.
        default: The value to return if `find_function` raises an AttributeError or TypeError.
                 Defaults to None.

    Returns:
        The result of `find_function()` if no exception occurs, otherwise returns `default`.

    Examples:
        >>> safe_extract(lambda: soup.find('div').text, default='Not found')
        'Extracted text or "Not found" if an exception occurs'
    """
    try:
        return find_function()
    except (AttributeError, TypeError):
        return default
    
def scrape_prodcuts_data(links:pd.DataFrame) -> str:
    """Extract Data From prodcuts Page
    
    Extract all relevant data from a prodcuts page provided by the link of the page including the id, the location, specific info about the product, and many more.
    
    Parameters
    ----------
    df : pd.DataFrame
        A Pandas DataFram that contains prodcuts web pages links, each link represent one listing, along with the price and the id of each listing.

    Returns
    -------
    str
        A message indicating the process of scraping products is complete
    """
    print(len(links))
    df = []
    i=1
    for row in range(len(links)):
        link = links.loc[row,'link']        
        r = requests.get(link)
        # make this a recursive function
        if str(r) == '<Response [403]>':
            print(r)
            print('Wating for an hour')
            time.sleep(3600)
            r = requests.get(link)
        soup = BeautifulSoup(r.content,'html.parser')
        
        data = {}
        data['link'] = link
        data['id'] = safe_extract(lambda: re.findall(r'\d+', soup.find('div', attrs={'class': re.compile('flex flexSpaceBetween alignItems pb-16 font-17 borderBottom')}).text)[0])
        data['title'] = safe_extract(lambda: soup.find('h1', attrs={'class': re.compile('postViewTitle font-22 mt-16 mb-32')}).text)
        data['images'] = safe_extract(lambda: [img['src'] for img in soup.find('div', {'class': 'image-gallery-slides'}).find_all('img')])
        data['description'] = safe_extract(lambda: soup.find('section', {'id': 'postViewDescription'}).div.text)
        data['owner'] = safe_extract(lambda: soup.find('section', {'id': 'PostViewOwnerCard'}).a.h3.text)
        data['google_maps_locatoin_link'] = safe_extract(lambda: soup.find('a', attrs={'class': re.compile('sc-750f6c2-0 dqtnfq map_google relative block mt-16')})['href'])
        coordinates = safe_extract(lambda: re.findall(r'-?\d+\.\d+', data['google_maps_locatoin_link']), [])
        data['long'] = coordinates[0] if coordinates else None
        data['lat'] = coordinates[1] if coordinates else None
        data['owner_link'] = safe_extract(lambda: 'https://opensooq.com' + soup.find('section', {'id': 'PostViewOwnerCard'}).a.get("href"))
        data['price'] = links.loc[row,'price']
        data['timestamp'] = datetime.datetime.now() # replace this one with the actual date that they put in open sooq
        # This for loop extracts all the data in the information section of the product's page including the building's rooms, bathrooms, age and more
        ul = soup.find('ul', attrs={'class':re.compile('flex flexSpaceBetween flexWrap mt-8')})
        try:
            for li in ul:
                data[li.p.text] = li.a.text
        except AttributeError:
            data[li.p.text] = li.find('p', attrs={'class':re.compile('width-75')}).text
        except TypeError:
            pass
        
        df.append(data)
        if i==len(links):
            pd.DataFrame(df).to_csv(f'data/incremental/products.csv',index_label=False, mode='w')
        i+=1
        if i%100==0:
            print(f'Scraped {i} products')
    return "Scraping Products is Done!"

def main():
    # ------Scraping Listings Links------
    URL = 'https://jo.opensooq.com/en/real-estate-for-sale/all?search=true&sort_code=recent'
    pages = 35
    scrape_links(URL, pages)
    # ------Scraping Products Data------
    links = pd.read_csv('data/incremental/links.csv')
    scrape_prodcuts_data(links)
    
if __name__=='__main__':
    main()