from kafka import KafkaProducer
import json
import praw

reddit = praw.Reddit(
    client_id="3VXzIQB5FBhi_80gtVmOtQ",
    client_secret="uaZilocmS0Pi9RRu6X3QX-j4GBniXQ",
    user_agent="my user app for peex",
)

producer = KafkaProducer(bootstrap_servers='localhost:9092',
value_serializer=lambda m: json.dumps(m).encode('ascii'))
for submission in reddit.subreddit("programming").hot(limit=10):
    transaction = {
        'id': submission.id,
        'title': submission.title,
        'upvote': str(submission.upvote)
    }
    producer.send('RedditProgramming', transaction)
    print(transaction)
