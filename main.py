import json
import requests
import csv
import datetime
import GetOldTweets3 as got
import os, sys, re, getopt
part = 2


BEFORE_TIME_DELTA = datetime.timedelta(days=-60)
AFTER_TIME_DELTA = datetime.timedelta(days=5)


input_file = 'input_part' + str(part) + '.csv'
count_file = 'count_tweets.csv'

def main():
    with open(input_file, encoding = "ISO-8859-1") as f:
        print('Open {}...'.format(input_file))
        reader = csv.DictReader(f)
        for row in reader:
            date = row['eventDate']
            name = row['CONML']
            account_name = row['twitter_account_name'][1:]
            if account_name == '':
                continue
            day, month, year = map(int, date.split('/'))
            year += 2000
            event_date = datetime.date(year, month, day)
            result_file_name = 'res_part{}/{}_{}.csv'.format(str(part), account_name, event_date.strftime('%Y%m%d'))
            start_date = event_date + BEFORE_TIME_DELTA
            end_date = event_date + AFTER_TIME_DELTA
            print('Company name: {}, company account: {}, event date: {}'.format(name, account_name, event_date.strftime("%Y-%m-%d")))
            print('Start date: {}, end date: {}'.format(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
            tweet_criteria = got.manager.TweetCriteria()
            tweet_criteria.querySearch = '@' + account_name
            tweet_criteria.since = start_date.strftime("%Y-%m-%d")
            tweet_criteria.until = end_date.strftime("%Y-%m-%d")
            print('Starting scrape tweets...')
            with open(result_file_name, "w+", encoding='utf-8') as result_file:
                result_file.write('date,username,to,retweets,favorites,text,geo,mentions,hashtags,id,permalink\n')
                cnt = 0
                def receiveBuffer(tweets):
                    nonlocal cnt
                    for t in tweets:
                        data = [t.date.strftime("%Y-%m-%d %H:%M:%S"),
                            t.username,
                            t.to or '',
                            t.retweets,
                            t.favorites,
                            '"'+t.text.replace('"','""')+'"',
                            t.geo,
                            t.mentions,
                            t.hashtags,
                            t.id,
                            t.permalink]
                        data[:] = [i if isinstance(i, str) else str(i) for i in data]
                        result_file.write(','.join(data) + '\n')
                    result_file.flush()
                    cnt += len(tweets)
                    if sys.stdout.isatty():
                        print("\rSaved %i"%cnt, end='', flush=True)
                    else:
                        print(cnt, end=' ', flush=True)
                got.manager.TweetManager.getTweets(tweet_criteria, receiveBuffer)
            print('{} {} Done'.format(account_name, event_date.strftime("%Y-%m-%d")))
if __name__ == '__main__':
    main()
