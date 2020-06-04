import os
import tweepy as tp
import pandas as pd

    
if __name__ == '__main__':
    
    consumer_key= 'homQ49cOTkr2ha90NWqrWE8Vi'
    consumer_secret= 'K3dv1ntjfWf4xQp5yeYb5APWcTlR3CJbnd3fHavMBAgPthKaq7'
    access_token= '1264991313725992962-6SkX7CPZrOaC3ZFwJzAEgvKgyI5InE'
    access_token_secret= 'dmhQ5GoCCfA02YBOO7Bn6M4ltYQO2mzMPvzAyrKsMybSe'
    auth = tp.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tp.API(auth, wait_on_rate_limit=True)
    
    search_words = "#BlueLivesMatter OR #BlueLivesMatters OR #WhiteLivesMatter"\
                    " OR #whiteoutwednesday OR #whiteoutwednsday"
    date_since = "2020-05-21"
    
    tweets = tp.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since).items(1000)
    
    
    expanded_tweets = [[tweet.user.screen_name, 
                        tweet.id,
                        tweet.user.id,
                        tweet.text, 
                        tweet.user.location, 
                        tweet.created_at,
                        tweet.favorite_count,
                        tweet.retweet_count,
                        tweet.user.followers_count,
                        "\n".join([media["media_url"] for media in\
                            tweet.extended_entities["media"]])\
                            if (hasattr(tweet, "extended_entities")) and \
                            ("media" in tweet.extended_entities) else "",
                        tweet.lang
                        ] for tweet in tweets]
    
    tweets_info = pd.DataFrame(data=expanded_tweets, 
                            columns=["user_name", 
                                    "tweet_id",
                                    "user_id",
                                    "text", 
                                    "location",
                                    "creation_time", 
                                    "favorite_count",
                                    "retweet_count",
                                    "followers_count",
                                    "media_url",
                                    "language"
                                    ])
    
    # sort the tweets according to popularity
    #tweets_info = tweets_info.sort_values(by=["retweet_count", \
    #                "followers_count", "favorite_count"], ascending=False)

    tweets_info.to_csv("data.csv", index=False)