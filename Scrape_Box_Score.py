# import os
# from bs4 import BeautifulSoup
# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
# import time

# SEASONS = list(range(2022,2025))

# DATA_DIR = "data"
# STANDINGS_DIR = os.path.join(DATA_DIR, "standings")

# # Check if the directory exists
# if not os.path.exists(STANDINGS_DIR):
#     # If the directory does not exist, create it
#     os.makedirs(STANDINGS_DIR)

# standings_files = os.listdir(STANDINGS_DIR)

# async def get_html(url, selector, sleep=5, retries=3):
#     html = None
#     for i in range(1, retries+1):
#         time.sleep(sleep * i)
#         try:
#             async with async_playwright() as p:
#                 browser = await p.firefox.launch(headless=False)
#                 page = await browser.new_page()
#                 await page.goto(url)
#                 html = await page.inner_html(selector=selector)
#         except PlaywrightTimeout:
#             print(f"Timeout error on {url}")
#             continue
#         else:
#             break
#     return html
# async def scrape_season(season):
#     url = f"https://www.basketball-reference.com/leagues/NBA_{season}_games.html"
#     html = await get_html(url, "#content .filter")
    
#     soup = BeautifulSoup(html)
#     links = soup.find_all("a")
#     standings_pages = [f"https://www.basketball-reference.com{l['href']}" for l in links]
    
#     for url in standings_pages:
#         save_path = os.path.join(STANDINGS_DIR, url.split("/")[-1])
#         if os.path.exists(save_path):
#             continue
        
#         html = await get_html(url, "#all_schedule")
#         with open(save_path, "w+") as f:
#             f.write(html)
# standings_files = os.listdir(STANDINGS_DIR)
# async def scrape_game(standings_file):
#     with open(standings_file, 'r') as f:
#         html = f.read()

#     soup = BeautifulSoup(html)
#     links = soup.find_all("a")
#     hrefs = [l.get('href') for l in links]
#     box_scores = [f"https://www.basketball-reference.com{l}" for l in hrefs if l and "boxscore" in l and '.html' in l]

#     for url in box_scores:
#         save_path = os.path.join(SCORES_DIR, url.split("/")[-1])
#         if os.path.exists(save_path):
#             continue

#         html = await get_html(url, "#content")
#         if not html:
#             continue
#         with open(save_path, "w+") as f:
#             f.write(html)

######## This is the working Portion of the code ########
# import os
# import asyncio
# from bs4 import BeautifulSoup
# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# SEASONS = list(range(2023,2025))

# DATA_DIR = "data"
# STANDINGS_DIR = os.path.join(DATA_DIR, "standings")
# SCORES_DIR = "scores"

# # Check if the directory exists
# if not os.path.exists(SCORES_DIR):
#     # If it doesn't exist, create it
#     os.makedirs(SCORES_DIR)

# # # Check if the directory exists
# if not os.path.exists(STANDINGS_DIR):
# #     # If the directory does not exist, create it
#    os.makedirs(STANDINGS_DIR)



# async def get_html(url, selector, sleep=5, retries=3):
#     html = None
#     for i in range(1, retries+1):
#         await asyncio.sleep(sleep * i)
#         try:
#             async with async_playwright() as p:
#                 browser = await p.firefox.launch(headless=False)
#                 page = await browser.new_page()
#                 await page.goto(url)
#                 html = await page.inner_html(selector=selector)
#         except PlaywrightTimeout:
#             print(f"Timeout error on {url}")
#             continue
#         else:
#             break
#     return html

# async def scrape_season(season):
#     url = f"https://www.basketball-reference.com/leagues/NBA_{season}_games.html"
#     html = await get_html(url, "#content .filter")
    
#     soup = BeautifulSoup(html, 'html.parser')
#     links = soup.find_all("a")
#     standings_pages = [f"https://www.basketball-reference.com{l['href']}" for l in links]
    
#     for url in standings_pages:
#         save_path = os.path.join(STANDINGS_DIR, url.split("/")[-1])
#         if os.path.exists(save_path):
#             continue
        
#         html = await get_html(url, "#all_schedule")
#         with open(save_path, "w+") as f:
#             f.write(html)

# async def main():
#     for season in SEASONS:
#         await scrape_season(season)

# # Run the main function
# asyncio.run(main())
 
#### This doesnt really work ####

# async def scrape_game(standings_file):
#     standings_files = os.listdir(STANDINGS_DIR)
#     for standings in standings_files:
#         full_path = os.path.join(STANDINGS_DIR, standings)
#         with open(full_path, 'r') as f:
#             html = f.read()

#         soup = BeautifulSoup(html)
#         links = soup.find_all("a")
#         hrefs = [l.get('href') for l in links]
#         box_scores = [l for l in hrefs if l and "boxscore" in l and '.html' in l]
#         box_scores = [f"https://www.basketball-reference.com{l}" for l in hrefs if l and "boxscore" in l and '.html' in l]
        
#         for url in box_scores:
#             save_path = os.path.join(SCORES_DIR, url.split("/")[-1])
#             if os.path.exists(save_path):
#                 continue

#             html = await get_html(url, "#content")
#             if not html:
#                 continue
#             with open(save_path, "w+") as f:
#                 f.write(html)
# async def main():
#     standings_files = os.listdir(STANDINGS_DIR)
#     for f in standings_files:
#         filepath = os.path.join(STANDINGS_DIR, f)
#         if os.path.exists(filepath):
#             continue
#         await scrape_game(filepath)

# # Run the main function
# asyncio.run(main())

### this also works ###
import os
import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

SEASONS = list(range(2024,2025))

DATA_DIR = "data"
STANDINGS_DIR = os.path.join(DATA_DIR, "standings")
SCORES_DIR = "scores"

# Check if the directory exists
if not os.path.exists(SCORES_DIR):
    # If it doesn't exist, create it
    os.makedirs(SCORES_DIR)

async def get_html(url, selector, sleep=5, retries=3):
    html = None
    for i in range(1, retries+1):
        await asyncio.sleep(sleep * i)
        try:
            async with async_playwright() as p:
                browser = await p.firefox.launch(headless=False)
                page = await browser.new_page()
                await page.goto(url)
                html = await page.inner_html(selector=selector)
        except PlaywrightTimeout:
            print(f"Timeout error on {url}")
            continue
        else:
            break
    return html

async def scrape_game(standings_file):
    with open(standings_file, 'r') as f:
        html = f.read()

    soup = BeautifulSoup(html)
    links = soup.find_all("a")
    hrefs = [l.get('href') for l in links]
    box_scores = [l for l in hrefs if l and "boxscore" in l and '.html' in l]
    box_scores = [f"https://www.basketball-reference.com{l}" for l in hrefs if l and "boxscore" in l and '.html' in l]
    
    for url in box_scores:
        save_path = os.path.join(SCORES_DIR, url.split("/")[-1])
        if os.path.exists(save_path):
            continue

        html = await get_html(url, "#content")
        if not html:
            continue
        with open(save_path, "w+", encoding='utf-8') as f:
            f.write(html)

async def main():
    standings_files = os.listdir(STANDINGS_DIR)
    for f in standings_files:
        filepath = os.path.join(STANDINGS_DIR, f)
        await scrape_game(filepath)

# Run the main function
asyncio.run(main())