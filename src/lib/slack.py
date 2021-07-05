import requests
import json


class Slack(object):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def post_error(self, msg):
        endpoint = self.config.slack_incomming_webhook_url
        payload = {"text": msg}
        ret = requests.post(endpoint, json.dumps(payload), headers={'Content-Type': 'application/json'})
        if ret.status_code == 200:
            self.logger.info("Success to post message to slack")
        else:
            self.logger.error(f"Fail to post slack. status={ret.status_code} text={ret.text}")
