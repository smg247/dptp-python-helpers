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
        return "Username: " + str(self.user) + "\n" + "Bot ID: " + str(self.bot_id) + "\n" + "Reply Count: " + str(
            self.reply_count) + "\n" + "Reply Users: " + str(self.reply_users_count) + "\n" + "Timestamp: " + str(
            self.time) + "\n"

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


class UserMessageCount:

    def __init__(self, user, post_count, reply_count) -> None:
        self.user = user
        self.name = ''
        self.email = ''
        self.full_name = ''
        self.post_count = post_count
        self.reply_count = reply_count

    def __str__(self) -> str:
        return "User Id: " + str(self.user) + "\n" + "Name:" + str(self.name) + "\n" + "Post Count: " + str(
            self.post_count) + "\n" + "Reply Count: " + str(self.reply_count)


def get_user_message_counts(start) -> {}:
    client = WebClient(token=os.environ.get("OAUTH_TOKEN"))

    users = {}
    next_cursor = None
    while True:
        history = client.conversations_history(channel=FORUM_CHANNEL,
                                               limit=1000,
                                               cursor=next_cursor,
                                               oldest=str(start.timestamp()))
        metadata = history.data.get('response_metadata')
        if metadata:
            next_cursor = metadata.get("next_cursor")
        else:
            next_cursor = None

        for message in history.data.get('messages'):
            if message.get('subtype') == 'bot_message':
                poster_id = None
                # Attempt to get the original poster of this workflow
                blocks = message.get('blocks')
                if len(blocks) > 0:
                    elements = blocks[0].get('elements')
                    if elements and len(elements) > 0:
                        inner_elements = elements[0].get('elements')
                        for e in inner_elements:
                            poster_id = e.get('user_id')
                            if poster_id:
                                user = users.get(poster_id)
                                if user:
                                    user.post_count += 1
                                else:
                                    user = UserMessageCount(user=poster_id, post_count=1, reply_count=0)
                                users[poster_id] = user
                                break

                # Get all users who replied to this workflow (other than the original poster)
                reply_users = message.get('reply_users')
                if reply_users:
                    for user_id in reply_users:
                        # We don't care about bots here so make sure it's an actual user
                        if user_id.startswith('U') and user_id != poster_id:
                            user = users.get(user_id)
                            if user:
                                user.reply_count += 1
                            else:
                                user = UserMessageCount(user=user_id, post_count=0, reply_count=1)
                            users[user_id] = user

        if next_cursor is None:
            break

    for user_id in users.keys():
        user_info = client.users_info(user=user_id)
        info = user_info.data.get('user')
        user = users.get(user_id)
        user.name = info.get('name')
        user.email = info.get('profile').get('email')
        user.full_name = info.get('real_name')

    return users


def convert_messages_to_dataframe(messages) -> pandas.DataFrame:
    fields = ['user', 'bot_id', 'reply_count', 'reply_users_count', 'month', 'day', 'year']
    return pd.DataFrame([{fn: getattr(m, fn) for fn in fields} for m in messages])


def convert_users_to_dataframe(users) -> pandas.DataFrame:
    fields = ['user', 'name', 'email', 'full_name', 'post_count', 'reply_count']
    return pd.DataFrame([{fn: getattr(u, fn) for fn in fields} for u in users])


if __name__ == '__main__':
    # messages = retrieve_messages(datetime(2022, 10, 1))
    # data_frame = convert_messages_to_dataframe(messages)
    # print(data_frame)
    # data_frame.to_clipboard(index=False, header=False)

    user_message_counts = get_user_message_counts(datetime(2022, 6, 1))
    data_frame = convert_users_to_dataframe(user_message_counts.values())
    print(data_frame)
    data_frame.to_clipboard(index=False)
