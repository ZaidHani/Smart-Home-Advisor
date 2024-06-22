from bs4 import BeautifulSoup
import requests
import os
import glob
import re
import pandas as pd
import time

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
        soup = BeautifulSoup(r.content)
        table = soup.find('section', attrs = {'id':'serpMainContent'})
        for row in table.findAll('div', {'class':re.compile('sc-21acf5d5-0 jhHVZS mb-32 relative radius-8 grayHoverBg whiteBg boxShadow2')}): 
            data = {}
            data['id'] = row.a['href'][11:20]
            data['link'] = 'https://opensooq.com'+row.a['href']
            data['price'] = row.find('div',{'class':'priceColor bold alignSelfCenter font-18 ms-auto'}).text
            df.append(data)
        if i%50==0:
            time.sleep(60)
            print(f'Pages Scrpaed: {i}')
        url = URL
    pd.DataFrame(df).to_csv('data/links.csv',index_label=False)
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
    for row in range(1, len(links)+1):
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
        data['member_since'] = safe_extract(lambda: soup.find('section', {'id': 'PostViewOwnerCard'}).find('span', {'class': 'ltr inline'}).text)
        data['description'] = safe_extract(lambda: soup.find('section', {'id': 'postViewDescription'}).div.text)
        data['owner'] = safe_extract(lambda: soup.find('section', {'id': 'PostViewOwnerCard'}).a.h3.text)
        data['reviews'] = safe_extract(lambda: soup.find('section', {'id': 'PostViewOwnerCard'}).a.span.text)
        data['google_maps_locatoin_link'] = safe_extract(lambda: soup.find('a', attrs={'class': re.compile('sc-750f6c2-0 dqtnfq map_google relative block mt-16')})['href'])
        coordinates = safe_extract(lambda: re.findall(r'-?\d+\.\d+', data['google_maps_locatoin_link']), [])
        data['long'] = coordinates[0] if coordinates else None
        data['lat'] = coordinates[1] if coordinates else None
        data['owner_link'] = safe_extract(lambda: 'https://opensooq.com' + soup.find('section', {'id': 'PostViewOwnerCard'}).a.get("href"))
        #
        data['price'] = links.loc[row,'price']
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
        if i%1000==0:
            print(f'Products Scraped: {i}')
            pd.DataFrame(df).to_csv(f'data/products/{(i//1000)}.csv',index_label=False)
            df = []
        elif i==len(links):
            pd.DataFrame(df).to_csv(f'data/products/{(i//1000)}.csv',index_label=False)
        i+=1
    return "Scraping Products is Done!"

def scrape_seller_data(links:list) -> str:
    """Extract Data From Seller Page
    
    Extract all relevant data from sellers page provided by the link of the page, and store them in 'data/sellers.csv' file.
    
    Parameters
    ----------
    links : list
        A list of sellers web pages links, each link represent one seller

    Returns
    -------
    str
        A message that indicates that scraping sellers data is done.
    """
    df = []
    print(f'Pulling {len(links)} Sellers Pages')
    for link in links:
        r = requests.get(link+'?info=info')
        soup = BeautifulSoup(r.content,'html.parser')
        data = {}
        data['owner_link'] = link
        data['owner'] = safe_extract(lambda: soup.find('h1', {'class':'font-24'}).text)
        popularity = safe_extract(lambda: soup.find('div',{'class':'flex mt-auto mb-32'}).find_all('span', {'class':'bold'}), [])
        data['views'] = popularity[0].text if popularity else None
        data['followers'] = popularity[1].text if popularity else None
        data['average_rating'] = safe_extract(lambda: soup.find('div',{'class':'sc-fb4b16ed-4 dkGjJX bold'}).text)
        data['google_maps_locatoin_link'] = safe_extract(lambda: soup.find('a', attrs={'class': re.compile('sc-750f6c2-0 dqtnfq map_google relative block mt-16')})['href'])
        coordinates = safe_extract(lambda: re.findall(r'-?\d+\.\d+', data['google_maps_locatoin_link']), [])
        data['long'] = coordinates[0] if coordinates else None
        data['lat'] = coordinates[1] if coordinates else None
        df.append(data)
    pd.DataFrame(df).to_csv('sellers.csv',index_label=False)
    return "Scraping Sellers Data is Done!"

def main():
    # ------Scraping Listings Links------
    # this has already been done so no need to run this function again.
    # URL = 'https://jo.opensooq.com/en/real-estate-for-sale/all?search=true&sort_code=recent'
    # pages = 1122 # this number might change, go to the link above and see how many pages are there
    # scrape_links(URL, pages)
    
    # ------Scraping Products Data------
    # redo the scraping, for 2 reasons:
        # 1- most of the data will be gone by the time you read this
        # 2- the data scraped is not complete, we are missing about 2K rows
    # links = pd.read_csv('data/links.csv')
    # scrape_prodcuts_data(links)

    # ------Scraping Sellers Data------
    all_files = glob.glob(os.path.join(r'.\data\products' , "*.csv"))
    li = []
    for filename in all_files:
        df = pd.read_csv(filename)
        li.append(df)
    sellers_links = pd.concat(li, axis=0, ignore_index=True)
    sellers_links = list(sellers_links['owner_link'].dropna().unique())
    scrape_seller_data(sellers_links)
    
if __name__=='__main__':
    main()