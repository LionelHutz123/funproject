import os
import pandas as pd
from bs4 import BeautifulSoup

SCORES_DIR = box_scores
box_scores = os.listdir(SCORES_DIR)
box_scores = [os.path.join(SCORES_DIR, f) for f in box_scores if f.endswith(".html")]   
def parse_html(box_scores):
    with open(box_scores) as f:
        html = f.read()
    soup = BeautifulSoup(html)
    [s.decompose() for s in soup.select("tr.over_header")]
    [s.decompose() for s in soup.select("tr.thead")]
    return soup
def read_season_info(soup):
    nav = soup.select("#bottom_nav_container")[0]
    hrefs = [a["href"] for a in nav.find_all('a')]
    season = os.path.basename(hrefs[1]).split("_")[0]
    return season
def read_line_score(soup):
    line_score = pd.read_html(str(soup), attrs={"id": "line_score"})[0]
    cols = list(line_score.colums)
    cols[0] = "team"
    cols [-1] = "total"
    line_score.columns = cols
    line_score = line_score[["team", "total"]]
    return line_score
def read_refs(soup):
    refs = pd.read_html(str(soup), attrs={"id": "officials"})[0]
    refs = refs.iloc[0,:].str.split(" - ", expand=True)
    refs.columns = ["ref_1", "ref_2", "ref_3"]
    return refs
def read_stats(soup, team, stat):
    df = pd.read_html(str(soup), attrs = {'id': f'box-{team}-game-{stat}'}, index_col=0)[0]
    df = df.apply(pd.to_numeric, errors="coerce")
    return df
games = []
base_cols = None
for box_score in box_scores:
    soup = parse_html(box_score)

    line_score = read_line_score(soup)
    teams = list(line_score["team"])
    refs = read_refs(soup)

    summaries = []
    for team in teams:
        basic = read_stats(soup, team, "basic")
        advanced = read_stats(soup, team, "advanced")
        refs = read_refs(soup)

        totals = pd.concat([basic.iloc[-1,:], advanced.iloc[-1,:]])
        totals.index = totals.index.str.lower()

        maxes = pd.concat([basic.iloc[:-1].max(), advanced.iloc[:-1].max()])
        maxes.index = maxes.index.str.lower() + "_max"

        summary = pd.concat([totals, maxes, refs])
        
        if base_cols is None:
            base_cols = list(summary.index.drop_duplicates(keep="first"))
            base_cols = [b for b in base_cols if "bpm" not in b]
        
        summary = summary[base_cols]
        
        summaries.append(summary)
    summary = pd.concat(summaries, axis=1).T

    game = pd.concat([summary, line_score, refs], axis=1)

    game["home"] = [0,1]

    game_opp = game.iloc[::-1].reset_index()
    game_opp.columns += "_opp"

    full_game = pd.concat([game, game_opp], axis=1)
    full_game["season"] = read_season_info(soup)
    
    full_game["date"] = os.path.basename(box_score)[:8]
    full_game["date"] = pd.to_datetime(full_game["date"], format="%Y%m%d")
    
    full_game["won"] = full_game["total"] > full_game["total_opp"]
    games.append(full_game)
    
    if len(games) % 100 == 0:
        print(f"{len(games)} / {len(box_scores)}")




