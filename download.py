import psaw
import pandas as pd

def download_comments(subreddit, chunk_size, after):

    api = psaw.PushshiftAPI()

    after = after

    while True:

        df = pd.DataFrame(
            api.search_comments(
            subreddit = subreddit,
            sort = 'asc',
            sort_type = 'created_utc',
            filter=['author','body','subreddit',
                    'score', 'level', 'submit_text',
                    'parent_id', 'link_id','id'],
            after = after,
            limit = chunk_size))

        start = df.iloc[0]['created_utc']
        end = df.iloc[-1]['created_utc']

        filename = 'comments/' + subreddit + '_comments_' + str(start) + '_' + str(end) + '.csv'
        df.to_csv(filename)

        after = end

        print(after)

        if len(df) < chunk_size:
            break

def download_submissions(subreddit, chunk_size, after):

    api = psaw.PushshiftAPI()

    after = after

    while True:

        df = pd.DataFrame(
            api.search_submissions(
                subreddit = subreddit,
                sort = 'asc',
                sort_type = 'created_utc',
                filter = ['id', 'url','author',
                    'title', 'subreddit', 'score',
                    'num_comments'],
                after = after,
                limit = chunk_size))

        start = df.iloc[0]['created_utc']
        end = df.iloc[-1]['created_utc']

        filename = 'posts/' + subreddit + '_posts_' + str(start) + '_' + str(end) + '.csv'
        df.to_csv(filename)

        after = end

        print(after)

        if len(df) < chunk_size:
            break
