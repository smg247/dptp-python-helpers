import os
import sys

import pandas
from slack_sdk import WebClient
from datetime import datetime, timedelta

import pandas as pd

# Enable debug logging
import logging

logging.basicConfig(level=logging.DEBUG)

FORUM_CHANNEL = "CBN38N3MW"


class Message:

    def __init__(self, user, bot_id, reply_count, reply_users_count, timestamp) -> None:
        self.user = user
        self.bot_id = bot_id
        self.reply_count = reply_count
        self.reply_users_count = reply_users_count
        if timestamp:
            time_without_fractions_of_second = str.split(timestamp, '.')[0]
            self.time = datetime.fromtimestamp(int(time_without_fractions_of_second))
            self.month = self.time.month
            self.year = self.time.year
            self.day = self.time.day

    def __str__(self) -> str:
        return "Username: " + str(self.user) + "\n" + "Bot ID: " + str(self.bot_id) + "\n" + "Reply Count: " + str(self.reply_count) + "\n" + "Reply Users: " + str(self.reply_users_count) + "\n" + "Timestamp: " + str(self.time) + "\n"

    def to_dataframe_list(self) -> []:
        return [self.user, self.bot_id, self.reply_count, self.reply_users_count, self.month, self.day, self.year]


def retrieve_messages(month) -> []:
    first_day = datetime(month.year, month.month, 1)
    some_day_next_month = first_day + timedelta(days=32)
    last_day = datetime(some_day_next_month.year, some_day_next_month.month, 1)

    client = WebClient(token=os.environ.get("OAUTH_TOKEN"))

    ret = []
    history = client.conversations_history(channel=FORUM_CHANNEL,
                                           limit=1000,
                                           latest=str(last_day.timestamp()),
                                           oldest=str(first_day.timestamp()))

    for message in history.data.get('messages'):
        user = message.get('username')
        if user is None:
            user = message.get('user')

        ret.append(Message(user=user,
                           bot_id=message.get('bot_id'),
                           reply_count=message.get('reply_count'),
                           reply_users_count=message.get('reply_users_count'),
                           timestamp=message.get('ts')))

    return ret


def convert_to_dataframe(messages) -> pandas.DataFrame:
    fields = ['user', 'bot_id', 'reply_count', 'reply_users_count', 'month', 'day', 'year']
    return pd.DataFrame([{fn: getattr(m, fn) for fn in fields} for m in messages])


if __name__ == '__main__':
    messages = retrieve_messages(datetime(2022, 9, 1))
    data_frame = convert_to_dataframe(messages)
    print(data_frame)
    data_frame.to_clipboard(index=False)


