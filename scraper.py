from dotenv import load_dotenv
import praw
from datetime import datetime
import os
import time
import pandas as pd
import logging 

logging.basicConfig(level = logging.INFO, format = "%(asctime)s [%(levelname)s] %(message)s")
load_dotenv('.env')
client_id = os.getenv('client_id')
client_secret = os.getenv('client_secret')
user_agent = os.getenv('user_agent')
subreddits = ['technology', 'science', 'entertainment', 'ArtificialInteligence', 'politics']
limit = 1000


def reddit_scraper(client_id,client_secret, user_agent, subreddit_name: str , limit:int, max_retries:int = 3):
    reddit = praw.Reddit(
        client_id= client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    subreddit = reddit.subreddit(subreddit_name)
    data = []
    logging.info(f'Starting to scrape r/{subreddit_name}')

    for post in subreddit.new(limit = limit):
        try:
                post_data = {
                    "id" : post.id,
                    "title" : post.title, 
                    "subreddit" : subreddit_name}
                
                data.append(post_data)

        except Exception as e:
                logging.error(f'Unable to scrape {post.title} due to {e}')
    logging.info(f'Scraped {len(data)} posts from r/{subreddit_name}')                       
    return data    

def create_dataframe(data, subreddit_name:str):
    post_rows = []
    for p in data:
        post_rows.append({
            'post_id' : p["id"],
            'title' : p["title"],
            'subreddit' : p["subreddit"]
            })
    df = pd.DataFrame(post_rows)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = f'./data/reddit_posts_{timestamp}.parquet'
    df.to_parquet(filepath, index=False)
    logging.info(f"Saved {len(df)} posts to {filepath}")
    return df

def main():
    total_data = []
    for subreddit in subreddits:
        data = reddit_scraper(client_id, client_secret, user_agent, subreddit, max_retries = 3, limit = limit)
        logging.info(f'Finished scraping r/{subreddit}')
        total_data.extend(data)
        time.sleep(10)
    df = create_dataframe(total_data, subreddit)

if __name__ == "__main__":
    main()







    