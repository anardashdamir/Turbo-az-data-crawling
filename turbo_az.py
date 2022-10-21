import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import time
from datetime import date
import threading

List, errors = [], []
exc_start = time()


def scrape(page):
    start = time()
    extras, info, baxish_sayi, yenilendi, elan_id, urls = [], [], [], [], [], []
    details_list = [[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]

    try:
        base_url = f'https://turbo.az/autos?page={page}'
        response = requests.get(base_url)
        soup = BeautifulSoup(response.content, 'lxml')
        container = soup.find_all('div', {'class': 'tz-container'})[8]
        products = container.find_all('div', {'class': 'products-i'})
        item_url = [product.find('a').get('href') for product in products]

        for url in item_url:
            soup = BeautifulSoup(requests.get('https://turbo.az' + url).content, 'html.parser')
            properties = soup.find('ul', {'class': 'product-properties'})
            properties_items = properties.find_all('div', {'class': 'product-properties-value'})

            stats = soup.find('div', {'class': 'product-statistics'}).find_all('p')
            baxish_sayi.append(stats[0].text)
            yenilendi.append(stats[1].text)
            elan_id.append(stats[2].text)

            extras_prob = soup.find_all('p', {'class': 'product-extras-i'})
            if len(extras_prob):
                extras.append([i.text for i in extras_prob])
            else:
                extras.append(None)

            info_text = soup.find('h2')
            if info_text:
                info.append(info_text.text)
            else:
                info.append(None)
            urls.append('https://turbo.az' + url)

            for index in range(14):
                details_list[index].append(properties_items[index].text)  # Save in List obj

        df_cols = ['sheher', 'marka', 'model', 'buraxilish_ili', 'ban_novu', 'reng', 'muherrik', 'muherrikin_gucu',
                   'yanacaq_novu', 'yurush', 'karobka', 'oturucu', 'yeni', 'qiymet']
        df = pd.DataFrame(columns=df_cols)

        # Saving data as DataFrame
        for index in range(14):
            df[df_cols[index]] = details_list[index]
        # extra columns
        df['elan_id'] = elan_id
        df['extras'] = extras
        df['info'] = info
        df['baxish_sayi'] = baxish_sayi
        df['url'] = urls

        List.append(df)

    except:
        errors.append(page)

    end = time()

    print(f'Page: {page}, Time: {end - start} s.')


# Threading part
# Define thread count and how many page you want to scrape

page_count = 417
thread_count = 35

arr = [*range(0, page_count, thread_count)]
length = [*range(page_count)]
i = 0

while True:
    all_threads = []
    try:
        for e in length[arr[i]:arr[i + 1]]:
            thread = threading.Thread(target=scrape, args=(e,))
            all_threads.append(thread)

        for thread in all_threads:
            thread.start()

        for thread in all_threads:
            thread.join()

        i += 1
    except:
        for e in length[arr[i]:]:
            thread = threading.Thread(target=scrape, args=(e,))
            all_threads.append(thread)

        for thread in all_threads:
            thread.start()

        for thread in all_threads:
            thread.join()
        break


class ConcatError(Exception):
    pass


# Convert to csv
today = date.today()
try:
    pd_df = pd.concat(List, ignore_index=True)
    pd_df.to_csv(f'turbo_az_{today}.csv', index=False, encoding='utf_8_sig')
except:
    err_msg = 'There is no dataframe to concat, Probably too many request from the same ip.Please try to Use VPN to ' \
              'solve it '
    raise ConcatError(err_msg)

# Extra execution details
finish = time()
print(f'Errors count: {len(errors)}')

if len(errors):
    print('Error Page: ', *sorted(errors))

print(f'Execution time: {finish - exc_start}')
