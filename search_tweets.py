from searchtweets import gen_rule_payload, load_credentials, collect_results


def main():
    premium_search_args = load_credentials("./twitter_keys.yaml",
                                       yaml_key="search_tweets_api",
                                       env_overwrite=False)

    rule = gen_rule_payload("COVID-19", results_per_call=100)
    tweets = collect_results(rule,
    
                            result_stream_args=premium_search_args)
    [print(tweet.all_text, end='\n\n') for tweet in tweets[0:10]]
    
if __name__ == '__main__':
    main()