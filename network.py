#!/usr/bin/python3

import sqlite3
import pandas as pd
import pathpy as pp
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime

# Maybe I will remove them soon
sqlite_comment_query = "SELECT * FROM comments WHERE subreddit=='{subreddit:s}'" 
sqlite_submission_query = "SELECT * FROM submissions WHERE subreddit=='{subreddit:s}'" 

seconds_per = dict(second=1, minute=60, hour=3600, day=86400, month=2.628e+6, year=3.154e+7)

# parent_id prefixes form https://www.reddit.com/dev/api/
t_values_dict = {
    't1': 'Comment',
    't2': 'Account',
    't3': 'Link',
    't4': 'Message',
    't5': 'Subreddit',
    't6': 'Award'
}


def create_comment_structure_graph(conn: sqlite3.Connection, subreddit: str, temporal: bool = False):  
    comment_df = pd.read_sql_query(
        "SELECT id, parent_id, created_utc FROM comments WHERE subreddit=='{subreddit:s}'" .format(subreddit=subreddit),
        conn
    )
    submission_df = pd.read_sql_query(
        "SELECT id, subreddit, created_utc FROM submissions WHERE subreddit=='{subreddit:s}'" .format(subreddit=subreddit),
        conn
    )
     
    if temporal:
        g = pp.TemporalNetwork()
    else:
        g = pp.Network(directed=True)
    
    for ind, record in comment_df.iterrows():
        source_id = record['id']
        t_value, target_id = record.parent_id.split('_')

        if temporal:
            ts = record.created_utc
            g.add_edge(source_id, target_id, ts)
        else:
            g.add_edge(source_id, target_id)

        # If the comment points at a submission also add edge to the subreddit.
        if t_value == 't3':
            source_id = target_id
            target_id = subreddit
            if temporal:
                ts = int(submission_df[submission_df['id'] == source_id].created_utc.iloc[0])
                g.add_edge(source_id, target_id, ts)
            else:
                g.add_edge(source_id, target_id)


    return g


def create_user_interaction_graph(conn: sqlite3.Connection, subreddit: str, temporal: bool = False):
    if temporal:
        g = pp.TemporalNetwork()
    else:
        g = pp.Network(directed=True)

    comment_df = pd.read_sql_query(
        "SELECT id, author, parent_id, created_utc FROM comments WHERE subreddit=='{sub:s}' AND author!='[deleted]'"
        .format(sub=subreddit),
        conn
    )
    submission_df = pd.read_sql_query(
        "SELECT id, author, created_utc FROM submissions WHERE subreddit=='{sub:s}'".format(sub=subreddit),
        conn
    )
    

    for ind, row in comment_df.iterrows():
        source_author = row['author']
        t_value, target_id = row['parent_id'].split('_')
        
        # If link / submission
        if t_value == 't3':
            target_author = submission_df[submission_df['id'] == target_id]['author']
        # If comment
        elif t_value == 't1':
            target_author = comment_df[comment_df['id'] == target_id]['author']
        # Everything else
        else:
            print('?')
            continue

        # Check if at the search gave at least one result
        if len(target_author) > 0:
            target_author = target_author.iloc[0]
            if temporal:
                ts = int(row['created_utc'])
                g.add_edge(source_author, target_author, ts)
            else:
                g.add_edge(source_author, target_author)

    return g


# VISUALIZATION
def visualize_graph(graph: pp.classes.network.Network):
    """
    This function just works in Jupyter Notebooks.
    """
    if hasattr(graph, 'time'):
        pp.visualisation.plot(
            graph,
            ms_per_frame=100,
            ts_per_frame=604800,
            look_behind=604800,
            look_ahead=604800, 
        )
    else:
        pp.visualisation.plot(
            network=graph
        )


def export_graph_to_html(graph: pp.classes.network.Network, filename: str, node_color='red'):
    if not filename.endswith('.html'):
        filename += '.html'
    
    if hasattr(graph, 'time'):
        pp.visualisation.export_html(
            graph,
            filename=filename,
            width=1000,
            height=1000,
            ms_per_frame=100,
            ts_per_frame=604800,
            look_behind=604800,
            look_ahead=604800,
            active_edge_color=node_color,
            active_node_color=node_color,
        )
    else:
        pp.visualisation.export_html(
            graph,
            filename=filename,
            width=1000,
            height=1000,
            node_color=node_color,
        )


def get_temporal_activity(conn: sqlite3.Connection, subreddit: str, freq_per: str = 'day'):
    all_times = []
    
    curs = conn.cursor()
    curs.execute(
        "SELECT created_utc FROM comments WHERE subreddit=='{subreddit:s}'".format(subreddit=subreddit)
    )
    all_times += [record[0] for record in curs]

    curs.execute(
        "SELECT created_utc FROM submissions WHERE subreddit=='{subreddit:s}'".format(subreddit=subreddit)
        )
    all_times += [record[0] for record in curs]

    hist, bin_edges = np.histogram(all_times, bins=200)
    bin_width = abs(bin_edges[1] - bin_edges[0])
    
    dates = np.array(.5 *(bin_edges[1:] + bin_edges[:-1]), dtype=int)
    freq = hist/bin_width * seconds_per[freq_per]

    return freq, dates 


def plot_temporal_activity(conn: sqlite3.Connection, subreddit_list: list or str, file_name: str = None):
    """
    Plot the number of posts per day, week or month
    :param curs: Sqlite3 cursor conntected of a database of comments and submissions.
    :param subreddit: The subreddit which should be plotted.
    :param file_name: If defined, the figure is saved at the end.
    """
    fig, ax = plt.subplots()

    if not isinstance(subreddit_list, (list, np.ndarray)):
        subreddit_list = [subreddit_list]

    for subreddit in subreddit_list:
        freq, dates = get_temporal_activity(conn, subreddit=subreddit)
        mpl_dates = mdates.epoch2num(dates)

        ax.plot_date(
            mpl_dates,
            freq,
            fmt='-',
            label=subreddit,
        )

    ax.set_xlabel(r'Date')
    ax.set_ylabel(r'Number of submissions & comments per day')

    # New automated ticks
    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)

    if len(subreddit_list) == 1: 
        ax.set_title('Subreddit "{subreddit:s}"'.format(subreddit=subreddit))
    else:
        ax.legend(
            loc='best',
            title=r'Subreddits',
            frameon=False,
        )

    if file_name:
        if not file_name.endswith(('.png','.pdf')):
            file_name += '.png'
        fig.savefig(file_name)
    else:
        fig.show()


# TODO Scatter plot in- VS. out-degree of every node (user). For that, the user graph function has to be fixed first.
def plot_in_vs_out_degree(g: pp.classes.Network):
    print('To be done soon by Moritz')


if __name__ == '__main__':

    print(':)')

    # Connect to the example database containing submissions and comments of for subreddits
    conn = sqlite3.connect('./example.db')
    
    # Get list of subreddit in the connected database
    curs = conn.cursor()
    curs.execute("SELECT DISTINCT subreddit FROM submissions")
    subreddit_list = [c[0] for c in curs]

    # TEST FUNCTIONS ON SUBREDDITS IN EXAMPLE.DB

    # g = create_comment_structure_graph(conn, subreddit_list[3], temporal=False)
    # export_graph_to_html(g, 'test_graph_static_comments')

    # h = create_comment_structure_graph(conn, subreddit_list[3], temporal=True)
    # export_graph_to_html(h, 'test_graph_temporal_comments')

    # i = create_user_interaction_graph(conn, "LibertarianPartyUSA", temporal=False)
    # export_graph_to_html(i, 'test_graph_static_users')

    # !!!THIS DOES NOT WORK AND I DO NOT KNOW WHY!!!
    # j = create_user_interaction_graph(conn, "LibertarianPartyUSA", temporal=True)
    # export_graph_to_html(j, 'test_graph_temporal_users')

    # plot_temporal_activity(conn, subreddit_list)
