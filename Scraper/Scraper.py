import argparse
from bs4 import BeautifulSoup as bsoup
import os
import pandas as pd
import re
import sys
import threading
from urllib.error import HTTPError, URLError
from urllib import parse
from urllib import request

import Const
sys.path.insert(1, '../Logging')
from Logging import get_logger

all_urls = dict()
url_lock = threading.Lock()
url_lock1 = threading.Lock()
reviews_pd = pd.DataFrame(columns=['title', 'rating', 'review'])


def get_product_urls(query, page_no, logger):
    global all_urls, url_lock

    def get_url(url):
        parsed_obj = parse.urlparse(url)
        return parsed_obj.geturl()

    url = get_url(Const.SEARCH_URL % (Const.BASE_URL, query, page_no))
    logger.info("[Thread-%s] Crawling URL: %s", str(page_no), url)
    try:
        html = request.urlopen(request.Request(url, **Const.HEADERS)).read()
    except (HTTPError, URLError) as error:
        logger.error('[Thread-%s] Data not retrieved because %s\nURL: %s',
                     page_no, error, url)
    page_soup = bsoup(html, "html.parser")
    containers = page_soup.findAll("a",
                                   {"class": "a-link-normal a-text-normal",
                                    "target": "_blank"})

    for container in containers:
        if not(container['href'].startswith('/gp') or
               'Test-Exclusive' in container['href']):
            name = container.text.strip('\n')
            with url_lock:
                all_urls[name] = Const.BASE_URL + str(container['href'])
                logger.info("%s : %s", name, all_urls[name])


def get_urls(keyword, logger):
    threads = list()
    for i in range(30):
        t = threading.Thread(target=get_product_urls,
                             args=(keyword, i+1, logger))
        t.daemon = True
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logger.info("Successfully finished all threads.")


def update_urls(logger):
    global all_urls
    for key in all_urls.keys():
        url = re.sub('/dp/', '/product-reviews/', all_urls[key])
        url_arr = url.split('/')
        url_arr[-1] = Const.REVIEW_URL
        all_urls[key] = '/'.join(url_arr)


def get_all_comments(key, url, logger):
    global reviews_pd, url_lock1
    for i in range(20):
        url = parse.urlparse(url + str(i+1)).geturl()
        logger.info("Scraping url %s for reviews", url)
        try:
            html = request.urlopen(request.Request(url,
                                                   **Const.HEADERS)).read()
            page_soup = bsoup(html, "html.parser")
            ratings = page_soup.findAll("span", {"class": "a-icon-alt"})
            reviews = page_soup.findAll("span", {"data-hook": "review-body"})
            for rating, review in zip(ratings, reviews):
                review1 = review.text.lstrip('\n\n ')
                review1 = review1.rstrip('\n\n')
                with url_lock1:
                    logger.info("Adding entry [%s, %s, %s]", key, rating.text,
                                review1)
                    reviews_pd = reviews_pd.append(pd.Series([key, rating.text,
                                                              review1],
                                                   index=reviews_pd.columns),
                                                   ignore_index=True)
        except Exception:
            continue


def scrape_reviews(num_threads, logger):
    j = 0
    global all_urls
    keys = list(all_urls.keys())
    while j < len(keys):
        threads = []
        if j + num_threads >= len(keys):
            num_threads = len(keys) - j
        for i in range(num_threads):
            key = keys[j+i]
            t = threading.Thread(target=get_all_comments,
                                 args=(key, all_urls[key], logger))
            t.daemon = True
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        logger.info("Finished Processing all threads")
        j += num_threads


def scrape():
    global reviews_pd
    parser = argparse.ArgumentParser(description='Scrape Amazon For Reviews')
    parser.add_argument('--keyword', default=str, help='Search Keyword',
                        required=True)
    parser.add_argument('--num_threads', default=10, type=int,
                        help='Number of Threads')
    parser.add_argument('--data_dir', default='../Data',
                        type=str, help='Path to store csv file')
    parser.add_argument('--log_dir', default='../Logs',
                        type=str, help='Path to store log file')
    args = parser.parse_args()

    if not os.path.exists(args.data_dir):
        os.makedirs(args.data_dir)

    if not os.path.exists(args.log_dir):
        os.makedirs(args.log_dir)

    log_file = os.path.join(args.log_dir, 'scrapping.log')
    logger = get_logger("Scraper", log_file)
    logger.info("Scraping Amazon for keyword %s", args.keyword)

    # Get urls for products from keyword search
    get_urls(args.keyword, logger)

    # Update urls to access reviews directly.
    update_urls(logger)

    # Scrape ratings and reviews from updated urls.
    scrape_reviews(args.num_threads, logger)

    csv_file = os.path.join(args.data_dir, Const.REVIEW_CSV)
    reviews_pd.to_csv(csv_file)

    logger.info("Successfully scraped the reviews.")


if __name__ == "__main__":
    scrape()
