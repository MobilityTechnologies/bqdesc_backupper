import requests
import json

class Slack(object):
    def __init__(self,config,logger):
        self.config = config
        self.logger = logger

    def post_error(self,msg):
        endpoint=self.config.slack_incomming_webhook_url
        payload = {"text": msg}
        requests.post(endpoint,json.dumps(payload),headers={'Content-Type': 'application/json'})