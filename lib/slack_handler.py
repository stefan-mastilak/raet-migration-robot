# REF: stefan.mastilak@visma.com

from slack import WebClient, WebhookClient
from slack.errors import SlackApiError


class SlackLogger(object):
    """
    Class for logging into the Slack channel.
    """

    def __init__(self, creds, channel):
        """
        :param creds: credentials for Slack API stored in LastPass password manager
        :param channel: slack channel where to post
        """
        self.channel = channel
        self.webhook_url = creds.get('url')  # webhook url is stored in the 'url' section in LastPass item
        self.token = creds.get('notes')  # token is stored in the 'notes' section in LastPass item
        self.client = WebClient(token=self.token)
        self.webhook = WebhookClient(url=self.webhook_url)

    def upload_message(self, msg):
        """
        Upload message to the slack channel.
        :param msg: message to be posted
        """
        try:
            response = self.webhook.send(text=msg)
            assert response.status_code == 200
            assert response.body == "ok"

        except SlackApiError as e:
            assert e.response["ok"] is False
            assert e.response["error"]
            print(f"Got an error: {e.response['error']}")

    def upload_file(self, filepath):
        """
        Upload file to the slack channel.
        :param filepath: full path to file
        """
        try:
            response = self.client.files_upload(
                channels=self.channel,
                file=filepath)
            assert response["file"]

        except SlackApiError as e:
            assert e.response["ok"] is False
            assert e.response["error"]
            print(f"Got an error: {e.response['error']}")
