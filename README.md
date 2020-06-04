# tweets-downloading

### Set Up
  1. install the following packages: pandas, tweepy
     ```console
     pip3 install pandas
     pip3 install tweepy
     ```
  2. replace the general api access keys (i.e., API key, API secret key, access token, access token secret) in search_tweets.py at lines 8-12 with valid ones
  3. change the searching configuration (e.g., language, date_since) by modifying arguements of tweepy.Cursor()
  4. change the number of tweets to be download by modifying arguments of tweepy.Cursor.items()


### Reference
  TweePy Documentation: http://docs.tweepy.org/en/latest/cursor_tutorial.html
