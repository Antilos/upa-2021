import re, requests
import pandas as pd
from bs4 import BeautifulSoup

def get_headers(base_url, archive_url, target):
    headers_list = []
    target_indices = []

    with requests.Session() as s:
            #set headers
            s.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'})
            
            nrpzs_df = pd.DataFrame()
            #get data from nrpzs
            r = s.get(archive_url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            if soup:
                links = soup.find_all("a", href=re.compile(r"export-sluzby-.*?-.*?.csv")) #find all links with data format in href
                num_links = len(links)
                for i, link in enumerate(links):
                    print(f"reading link {i+1}/{num_links}")
                    #download data
                    href = link.get('href')
                    with requests.Session() as s2:
                        s2.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'})
                        r2 = s2.get(base_url+link.get('href'))
                        r2.raise_for_status()

                    decoded_content = r2.content.decode('latin2')
                    #get first line (spliting on eol too slow)
                    headers = ""
                    for c in decoded_content:
                        if c == '\n':
                            break
                        else:
                            headers += c
                    headers = list(map(lambda x : x.strip('"'), headers.split(';')))
                    date = re.search(r'export-sluzby-(.*?-.*?).csv', href)[1]

                    if i == 0:
                        for col in target:
                            target_indices.append(headers.index(col))
                        headers_list.append(tuple([date, target]))
                    else:
                        i_target = []
                        for i in target_indices:
                            i_target.append(headers[i])
                        headers_list.append(tuple([date, i_target]))
    return headers_list

def aggregate_by_target(headers, target):
    new_headers = []
    if not target is set:
        target = set(target)

    for i_dates, i_headers in headers:
        if target.issubset(set(i_headers)):
            new_headers.append([i_dates, target])
        else:
            new_headers.append([i_dates, target])

    for i_dates, i_headers in headers:
        for j_dates, j_headers in headers:
            ...
    return


def main():
    target_headers = ['ZdravotnickeZarizeniId', 'NazevCely', 'Kraj', 'KrajKod', 'Okres', 'OkresKod', 'DatumZahajeniCinnosti', 'OborPece']
    headers = get_headers('https://nrpzs.uzis.cz/', 'https://nrpzs.uzis.cz/index.php?pg=home--download&archiv=sluzby', target_headers)
    for date, cols in headers:
        print(date)
        print(cols)
        print('-------')

if __name__ == '__main__':
    main()